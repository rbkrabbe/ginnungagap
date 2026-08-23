use opentelemetry::global;
use opentelemetry::propagation::Injector;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use std::sync::Arc;

use ggap_proto::v1::{raft_service_client::RaftServiceClient, RaftMessage};
use ggap_types::{GgapError, ShardId};
use openraft::{
    error::{NetworkError, RPCError, RaftError, Unreachable},
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
    AnyError, RaftNetwork, RaftNetworkFactory,
};
use tonic::transport::Channel;

use crate::config::{GgapNode, GgapTypeConfig};
use crate::convert::{decode, encode};
use crate::registry::ShardRegistry;

// ---------------------------------------------------------------------------
// MetadataInjector — injects OTel context into outbound tonic request metadata
// ---------------------------------------------------------------------------

struct MetadataInjector<'a>(&'a mut tonic::metadata::MetadataMap);

impl<'a> Injector for MetadataInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = tonic::metadata::MetadataValue::try_from(&value) {
                self.0.insert(name, val);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GgapNetworkFactory
// ---------------------------------------------------------------------------

pub struct GgapNetworkFactory {
    pub shard_id: ShardId,
    /// Where every outbound RPC resolves its target address.
    registry: Arc<ShardRegistry>,
}

impl GgapNetworkFactory {
    pub fn new(shard_id: ShardId, registry: Arc<ShardRegistry>) -> Self {
        GgapNetworkFactory { shard_id, registry }
    }
}

impl RaftNetworkFactory<GgapTypeConfig> for GgapNetworkFactory {
    type Network = GgapNetwork;

    /// The `GgapNode` is ignored. Membership still carries addresses, but the
    /// network path takes none of them: the client holds the target's *id* and
    /// resolves it through the directory on every send.
    async fn new_client(&mut self, target_id: u64, _node: &GgapNode) -> GgapNetwork {
        GgapNetwork {
            target_id,
            registry: self.registry.clone(),
            connected: None,
            shard_id: self.shard_id,
        }
    }
}

// ---------------------------------------------------------------------------
// GgapNetwork
// ---------------------------------------------------------------------------

pub struct GgapNetwork {
    /// The node this client talks to. An id, not an address: where that node
    /// lives is a directory lookup, and the answer can change under us.
    target_id: u64,
    registry: Arc<ShardRegistry>,
    /// The address last dialled and its channel. Nothing here is a cached
    /// *resolution* — [`Self::resolve`] runs on every RPC — it only spares a
    /// re-dial while the answer is unchanged.
    connected: Option<(String, RaftServiceClient<Channel>)>,
    shard_id: ShardId,
}

impl GgapNetwork {
    /// The cluster address the next RPC will dial, or `None` when the directory
    /// cannot resolve the target.
    async fn resolve(&self) -> Option<String> {
        self.registry.directory_addr(self.target_id).await
    }

    /// Whether the channel in hand can serve `addr`. A channel is bound to the
    /// address it was dialled at, so a node that moved needs a new one — and
    /// openraft never rebuilds the client, so this is the only place that can
    /// notice.
    fn needs_redial(&self, addr: &str) -> bool {
        !matches!(&self.connected, Some((dialled, _)) if dialled == addr)
    }

    /// Resolve the target and return a client connected to it.
    ///
    /// An unresolvable id is an error, not a wait: it behaves exactly like an
    /// unreachable peer, which openraft already backs off and retries.
    async fn connect(&mut self) -> Result<&mut RaftServiceClient<Channel>, GgapError> {
        let addr = self.resolve().await.ok_or_else(|| {
            GgapError::Consensus(format!(
                "no directory entry for node {}: cannot resolve an address",
                self.target_id
            ))
        })?;

        if self.needs_redial(&addr) {
            let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .map_err(|e| GgapError::Consensus(e.to_string()))?
                .connect()
                .await
                .map_err(|e| GgapError::Consensus(e.to_string()))?;
            self.connected = Some((addr, RaftServiceClient::new(ch)));
        }

        Ok(&mut self.connected.as_mut().expect("just populated").1)
    }

    fn to_net_err(e: impl std::fmt::Display) -> RPCError<u64, GgapNode, RaftError<u64>> {
        RPCError::Network(NetworkError::new(&AnyError::error(e.to_string())))
    }

    fn to_unreachable(e: impl std::fmt::Display) -> RPCError<u64, GgapNode, RaftError<u64>> {
        RPCError::Unreachable(Unreachable::new(&AnyError::error(e.to_string())))
    }

    fn to_iss_unreachable(
        e: impl std::fmt::Display,
    ) -> RPCError<u64, GgapNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
        RPCError::Unreachable(Unreachable::new(&AnyError::error(e.to_string())))
    }

    fn to_iss_net_err(
        e: impl std::fmt::Display,
    ) -> RPCError<u64, GgapNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
        RPCError::Network(NetworkError::new(&AnyError::error(e.to_string())))
    }
}

#[allow(clippy::result_large_err)]
impl RaftNetwork<GgapTypeConfig> for GgapNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GgapTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, GgapNode, RaftError<u64>>> {
        let span = tracing::info_span!(
            "rpc.client.append_entries",
            otel.kind = "client",
            "rpc.system" = "grpc",
            "rpc.method" = "ginnungagap.v1.RaftService/AppendEntries",
            shard_id = self.shard_id,
        );

        async move {
            let payload = encode(&rpc).map_err(Self::to_net_err)?;

            let shard_id = self.shard_id;
            let client = self.connect().await.map_err(Self::to_unreachable)?;

            let mut req = tonic::Request::new(RaftMessage {
                shard_id,
                data: payload,
            });
            let cx = tracing::Span::current().context();
            global::get_text_map_propagator(|p| {
                p.inject_context(&cx, &mut MetadataInjector(req.metadata_mut()));
            });

            let resp = client
                .append_entries(req)
                .await
                .map_err(Self::to_unreachable)?
                .into_inner();

            decode::<AppendEntriesResponse<u64>>(&resp.data).map_err(Self::to_net_err)
        }
        .instrument(span)
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, GgapNode, RaftError<u64>>> {
        let span = tracing::info_span!(
            "rpc.client.vote",
            otel.kind = "client",
            "rpc.system" = "grpc",
            "rpc.method" = "ginnungagap.v1.RaftService/Vote",
            shard_id = self.shard_id,
        );

        async move {
            let payload = encode(&rpc).map_err(Self::to_net_err)?;

            let shard_id = self.shard_id;
            let client = self.connect().await.map_err(Self::to_unreachable)?;

            let mut req = tonic::Request::new(RaftMessage {
                shard_id,
                data: payload,
            });
            let cx = tracing::Span::current().context();
            global::get_text_map_propagator(|p| {
                p.inject_context(&cx, &mut MetadataInjector(req.metadata_mut()));
            });

            let resp = client
                .vote(req)
                .await
                .map_err(Self::to_unreachable)?
                .into_inner();

            decode::<VoteResponse<u64>>(&resp.data).map_err(Self::to_net_err)
        }
        .instrument(span)
        .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<GgapTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, GgapNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        let span = tracing::info_span!(
            "rpc.client.install_snapshot",
            otel.kind = "client",
            "rpc.system" = "grpc",
            "rpc.method" = "ginnungagap.v1.RaftService/InstallSnapshot",
            shard_id = self.shard_id,
        );

        async move {
            let payload = encode(&rpc).map_err(Self::to_iss_net_err)?;

            // Build the first (and only) message with injected trace context.
            let mut first = tonic::Request::new(RaftMessage {
                shard_id: self.shard_id,
                data: payload,
            });
            let cx = tracing::Span::current().context();
            global::get_text_map_propagator(|p| {
                p.inject_context(&cx, &mut MetadataInjector(first.metadata_mut()));
            });

            let client = self.connect().await.map_err(Self::to_iss_unreachable)?;

            // install_snapshot takes a streaming request; wrap the single
            // message in a stream (trace context is in the metadata of the
            // request, not per-message).
            let inner = first.into_inner();
            let stream = tokio_stream::iter(vec![inner]);

            let resp = client
                .install_snapshot(stream)
                .await
                .map_err(Self::to_iss_unreachable)?
                .into_inner();

            decode::<openraft::raft::InstallSnapshotResponse<u64>>(&resp.data)
                .map_err(Self::to_iss_net_err)
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ggap_types::{NodeAddrs, NodeDescriptor};

    async fn factory_with(entries: Vec<(u64, NodeDescriptor)>) -> GgapNetworkFactory {
        let registry = Arc::new(ShardRegistry::new(1, []));
        registry.merge_directory(entries).await;
        GgapNetworkFactory::new(0, registry)
    }

    /// The assertion that makes the next task a deletion rather than a fix:
    /// membership still carries both addresses, and the network path reads
    /// neither. A `GgapNode` pointing somewhere else changes nothing.
    #[tokio::test]
    async fn new_client_ignores_the_address_in_membership() {
        let mut factory = factory_with(vec![(
            2,
            NodeDescriptor::hint(NodeAddrs::cluster_only("directory:17001")),
        )])
        .await;

        let net = factory
            .new_client(2, &GgapNode::cluster_only("membership:17001"))
            .await;

        assert_eq!(net.resolve().await, Some("directory:17001".into()));
    }

    /// Resolution happens per RPC, so a descriptor that lands between two sends
    /// redirects the second one — openraft never rebuilds the client.
    #[tokio::test]
    async fn a_changed_directory_entry_redirects_the_next_send() {
        let mut factory = factory_with(vec![(
            2,
            NodeDescriptor::new(NodeAddrs::cluster_only("old:17001"), 1),
        )])
        .await;
        let net = factory.new_client(2, &GgapNode::default()).await;
        assert_eq!(net.resolve().await, Some("old:17001".into()));

        net.registry
            .merge_directory([(
                2,
                NodeDescriptor::new(NodeAddrs::cluster_only("moved:17001"), 2),
            )])
            .await;

        assert_eq!(net.resolve().await, Some("moved:17001".into()));
    }

    /// The re-dial decision itself, which the test above stops one step short
    /// of: a channel is bound to the address it was dialled at, so an unchanged
    /// address reuses it and a changed one throws it away. `connect_lazy` builds
    /// a channel without any I/O, which is all this branch needs to see.
    #[tokio::test]
    async fn a_channel_is_reused_only_for_the_address_it_was_dialled_at() {
        let mut factory = factory_with(vec![]).await;
        let mut net = factory.new_client(2, &GgapNode::default()).await;

        assert!(
            net.needs_redial("old:17001"),
            "a client that has never dialled must dial"
        );

        let lazy = Channel::from_static("http://old:17001").connect_lazy();
        net.connected = Some(("old:17001".to_string(), RaftServiceClient::new(lazy)));

        assert!(!net.needs_redial("old:17001"), "same address, same channel");
        assert!(
            net.needs_redial("moved:17001"),
            "the node moved: the old channel cannot reach it"
        );
    }

    /// A minimal `RaftService` so a test can dial something real. It answers
    /// nothing useful — the assertions are about *where* the client connected.
    #[derive(Default)]
    struct EchoRaft;

    #[tonic::async_trait]
    impl ggap_proto::v1::raft_service_server::RaftService for EchoRaft {
        async fn append_entries(
            &self,
            req: tonic::Request<RaftMessage>,
        ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
            Ok(tonic::Response::new(req.into_inner()))
        }

        async fn vote(
            &self,
            req: tonic::Request<RaftMessage>,
        ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
            Ok(tonic::Response::new(req.into_inner()))
        }

        async fn install_snapshot(
            &self,
            _req: tonic::Request<tonic::Streaming<RaftMessage>>,
        ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
            Ok(tonic::Response::new(RaftMessage::default()))
        }
    }

    /// Serve `EchoRaft` on an ephemeral port and return the address.
    async fn serve_raft() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ggap_proto::v1::raft_service_server::RaftServiceServer::new(
                    EchoRaft,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        });
        addr
    }

    /// The acceptance criterion end to end: one client, two `connect()` calls,
    /// a descriptor landing between them, and the second call connected to the
    /// new address. Both halves have to run through `connect()` — resolving and
    /// re-dialling are each pinned above, but only their composition here can
    /// catch an address cached between the two.
    #[tokio::test]
    async fn connect_follows_the_target_to_its_new_address() {
        let first = serve_raft().await;
        let second = serve_raft().await;
        assert_ne!(first, second);

        let mut factory = factory_with(vec![(
            2,
            NodeDescriptor::new(NodeAddrs::cluster_only(first.clone()), 1),
        )])
        .await;
        let mut net = factory.new_client(2, &GgapNode::default()).await;

        net.connect()
            .await
            .expect("first dial should reach the first server");
        assert_eq!(net.connected.as_ref().unwrap().0, first);

        net.registry
            .merge_directory([(
                2,
                NodeDescriptor::new(NodeAddrs::cluster_only(second.clone()), 2),
            )])
            .await;

        net.connect()
            .await
            .expect("second dial should reach the second server");
        assert_eq!(
            net.connected.as_ref().unwrap().0,
            second,
            "the client stayed on the address it first resolved"
        );
    }

    /// The same composition from the failure side, which is the half that bites
    /// in production: a target that moved somewhere unreachable must not keep
    /// being served by the channel to where it used to be.
    #[tokio::test]
    async fn a_moved_target_is_not_served_by_the_old_channel() {
        let reachable = serve_raft().await;
        let mut factory = factory_with(vec![(
            2,
            NodeDescriptor::new(NodeAddrs::cluster_only(reachable.clone()), 1),
        )])
        .await;
        let mut net = factory.new_client(2, &GgapNode::default()).await;
        net.connect().await.expect("first dial should succeed");

        // Port 1 is privileged and unbound, so this dial cannot succeed.
        net.registry
            .merge_directory([(
                2,
                NodeDescriptor::new(NodeAddrs::cluster_only("127.0.0.1:1"), 2),
            )])
            .await;

        assert!(
            net.connect().await.is_err(),
            "the old channel was reused for a target that has moved"
        );
    }

    /// An id the directory cannot resolve fails the RPC rather than waiting for
    /// one to appear: to openraft it is an unreachable peer, which it already
    /// backs off and retries.
    #[tokio::test]
    async fn an_unresolvable_target_fails_the_send() {
        let mut factory = factory_with(vec![]).await;
        let mut net = factory
            .new_client(2, &GgapNode::cluster_only("membership:17001"))
            .await;

        assert_eq!(net.resolve().await, None);
        let err = net
            .connect()
            .await
            .expect_err("must not dial an unknown node");
        assert!(
            err.to_string().contains("no directory entry for node 2"),
            "unexpected error: {err}"
        );
    }
}
