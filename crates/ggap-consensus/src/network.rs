use opentelemetry::global;
use opentelemetry::propagation::Injector;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use ggap_proto::v1::{raft_service_client::RaftServiceClient, RaftMessage};
use ggap_types::{GgapError, ShardId};
use openraft::{
    error::{NetworkError, RPCError, RaftError, Unreachable},
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
    AnyError, BasicNode, RaftNetwork, RaftNetworkFactory,
};
use tonic::transport::Channel;

use crate::config::GgapTypeConfig;
use crate::convert::{decode, encode};

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
}

impl GgapNetworkFactory {
    pub fn new(shard_id: ShardId) -> Self {
        GgapNetworkFactory { shard_id }
    }
}

impl RaftNetworkFactory<GgapTypeConfig> for GgapNetworkFactory {
    type Network = GgapNetwork;

    async fn new_client(&mut self, _target_id: u64, node: &BasicNode) -> GgapNetwork {
        GgapNetwork {
            addr: node.addr.clone(),
            channel: None,
            shard_id: self.shard_id,
        }
    }
}

// ---------------------------------------------------------------------------
// GgapNetwork
// ---------------------------------------------------------------------------

pub struct GgapNetwork {
    addr: String,
    channel: Option<RaftServiceClient<Channel>>,
    shard_id: ShardId,
}

impl GgapNetwork {
    async fn connect(&mut self) -> Result<(), GgapError> {
        if self.channel.is_none() {
            let endpoint = format!("http://{}", self.addr);
            let ch = tonic::transport::Endpoint::from_shared(endpoint)
                .map_err(|e| GgapError::Consensus(e.to_string()))?
                .connect()
                .await
                .map_err(|e| GgapError::Consensus(e.to_string()))?;
            self.channel = Some(RaftServiceClient::new(ch));
        }
        Ok(())
    }

    fn to_net_err(e: impl std::fmt::Display) -> RPCError<u64, BasicNode, RaftError<u64>> {
        RPCError::Network(NetworkError::new(&AnyError::error(e.to_string())))
    }

    fn to_unreachable(e: impl std::fmt::Display) -> RPCError<u64, BasicNode, RaftError<u64>> {
        RPCError::Unreachable(Unreachable::new(&AnyError::error(e.to_string())))
    }

    fn to_iss_unreachable(
        e: impl std::fmt::Display,
    ) -> RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
        RPCError::Unreachable(Unreachable::new(&AnyError::error(e.to_string())))
    }

    fn to_iss_net_err(
        e: impl std::fmt::Display,
    ) -> RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
        RPCError::Network(NetworkError::new(&AnyError::error(e.to_string())))
    }
}

#[allow(clippy::result_large_err)]
impl RaftNetwork<GgapTypeConfig> for GgapNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GgapTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let span = tracing::info_span!(
            "rpc.client.append_entries",
            otel.kind = "client",
            "rpc.system" = "grpc",
            "rpc.method" = "ginnungagap.v1.RaftService/AppendEntries",
            shard_id = self.shard_id,
        );

        async move {
            let payload = encode(&rpc).map_err(Self::to_net_err)?;

            self.connect().await.map_err(Self::to_unreachable)?;
            let client = self
                .channel
                .as_mut()
                .ok_or_else(|| Self::to_unreachable("channel not connected after connect()"))?;

            let mut req = tonic::Request::new(RaftMessage {
                shard_id: self.shard_id,
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
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let span = tracing::info_span!(
            "rpc.client.vote",
            otel.kind = "client",
            "rpc.system" = "grpc",
            "rpc.method" = "ginnungagap.v1.RaftService/Vote",
            shard_id = self.shard_id,
        );

        async move {
            let payload = encode(&rpc).map_err(Self::to_net_err)?;

            self.connect().await.map_err(Self::to_unreachable)?;
            let client = self
                .channel
                .as_mut()
                .ok_or_else(|| Self::to_unreachable("channel not connected after connect()"))?;

            let mut req = tonic::Request::new(RaftMessage {
                shard_id: self.shard_id,
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
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
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

            self.connect().await.map_err(Self::to_iss_unreachable)?;
            let client = self
                .channel
                .as_mut()
                .ok_or_else(|| Self::to_iss_unreachable("channel not connected after connect()"))?;

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
