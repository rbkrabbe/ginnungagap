use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ggap_consensus::{RaftNode, ShardRouter};
use ggap_proto::v1::{
    kv_service_server::KvService, watch_request, CasRequest, CasResponse, DeleteRequest,
    DeleteResponse, EventType, GetRequest, GetResponse, PutRequest, PutResponse, ScanRequest,
    ScanResponse, WatchEvent, WatchRequest,
};
use ggap_types::{DomainWatchEvent, KvCommand, WatchEventKind};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::convert::{
    ggap_to_status, kv_entry_to_proto, proto_read_consistency, proto_write_quorum, stub_header,
};

/// Global monotonic counter for assigning unique watch IDs.
static NEXT_WATCH_ID: AtomicU64 = AtomicU64::new(1);

/// Maximum scan limit to prevent resource exhaustion from unbounded scans.
const MAX_SCAN_LIMIT: u32 = 10_000;

pub struct KvServiceImpl {
    router: Arc<ShardRouter>,
    node_id: u64,
    max_key_bytes: usize,
    max_value_bytes: usize,
    watch_tx: Option<tokio::sync::broadcast::Sender<DomainWatchEvent>>,
    watch_output_buffer: usize,
}

impl KvServiceImpl {
    pub fn new(
        router: Arc<ShardRouter>,
        node_id: u64,
        max_key_bytes: usize,
        max_value_bytes: usize,
        watch_tx: Option<tokio::sync::broadcast::Sender<DomainWatchEvent>>,
        watch_output_buffer: usize,
    ) -> Self {
        KvServiceImpl {
            router,
            node_id,
            max_key_bytes,
            max_value_bytes,
            watch_tx,
            watch_output_buffer,
        }
    }
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        let mode = proto_read_consistency(req.consistency);
        let node = self
            .router
            .route_read(&req.key)
            .await
            .map_err(ggap_to_status)?;
        let entry = node
            .read(&req.key, req.at_version, mode)
            .await
            .map_err(ggap_to_status)?;

        match entry {
            None => Err(Status::not_found(format!("key '{}' not found", req.key))),
            Some(e) => Ok(Response::new(GetResponse {
                header: Some(stub_header(self.node_id)),
                kv: Some(kv_entry_to_proto(e)),
            })),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        if req.key.len() > self.max_key_bytes {
            return Err(Status::invalid_argument(format!(
                "key exceeds maximum size of {} bytes",
                self.max_key_bytes
            )));
        }
        if req.value.len() > self.max_value_bytes {
            return Err(Status::invalid_argument(format!(
                "value exceeds maximum size of {} bytes",
                self.max_value_bytes
            )));
        }
        let mode = proto_write_quorum(req.quorum);
        let ttl_ns = if req.ttl_secs == 0 {
            None
        } else {
            Some(req.ttl_secs as i64 * 1_000_000_000)
        };
        let node = self
            .router
            .route_write(&req.key)
            .await
            .map_err(ggap_to_status)?;
        let cmd = KvCommand::Put {
            key: req.key,
            value: req.value,
            ttl_ns,
            expect_version: req.expect_version,
        };
        let resp = node.propose(cmd, mode).await.map_err(ggap_to_status)?;
        match resp {
            ggap_types::KvResponse::Written { version } => Ok(Response::new(PutResponse {
                header: Some(stub_header(self.node_id)),
                new_version: version,
            })),
            ggap_types::KvResponse::Conflict { expected, actual } => Err(Status::aborted(format!(
                "version conflict: expected {expected}, got {actual}"
            ))),
            ggap_types::KvResponse::NoOp => unreachable!("Put returned NoOp"),
            _ => Err(Status::internal("unexpected response variant")),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        let mode = proto_write_quorum(req.quorum);
        let node = self
            .router
            .route_write(&req.key)
            .await
            .map_err(ggap_to_status)?;
        let cmd = KvCommand::Delete { key: req.key };
        let resp = node.propose(cmd, mode).await.map_err(ggap_to_status)?;
        match resp {
            ggap_types::KvResponse::Deleted { found } => Ok(Response::new(DeleteResponse {
                header: Some(stub_header(self.node_id)),
                found,
            })),
            ggap_types::KvResponse::NoOp => unreachable!("Delete returned NoOp"),
            _ => Err(Status::internal("unexpected response variant")),
        }
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        let req = request.into_inner();
        let start_key = if req.page_token.is_empty() {
            req.start_key.clone()
        } else {
            String::from_utf8(req.page_token.to_vec())
                .map_err(|_| Status::invalid_argument("invalid page_token: not valid UTF-8"))?
        };
        let mode = proto_read_consistency(req.consistency);
        let limit = if req.limit == 0 {
            MAX_SCAN_LIMIT
        } else {
            req.limit.min(MAX_SCAN_LIMIT)
        };
        let node = self
            .router
            .route_scan(&start_key, &req.end_key)
            .await
            .map_err(ggap_to_status)?;
        let (entries, continuation) = node
            .scan(&start_key, &req.end_key, limit, mode)
            .await
            .map_err(ggap_to_status)?;

        let kvs = entries.into_iter().map(kv_entry_to_proto).collect();
        let next_page_token = continuation.map(|k| k.into_bytes()).unwrap_or_default();

        Ok(Response::new(ScanResponse {
            header: Some(stub_header(self.node_id)),
            kvs,
            next_page_token,
        }))
    }

    async fn compare_and_swap(
        &self,
        request: Request<CasRequest>,
    ) -> Result<Response<CasResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        if req.key.len() > self.max_key_bytes {
            return Err(Status::invalid_argument(format!(
                "key exceeds maximum size of {} bytes",
                self.max_key_bytes
            )));
        }
        if req.new_value.len() > self.max_value_bytes {
            return Err(Status::invalid_argument(format!(
                "value exceeds maximum size of {} bytes",
                self.max_value_bytes
            )));
        }
        let mode = proto_write_quorum(req.quorum);
        let ttl_ns = if req.ttl_secs == 0 {
            None
        } else {
            Some(req.ttl_secs as i64 * 1_000_000_000)
        };
        let node = self
            .router
            .route_write(&req.key)
            .await
            .map_err(ggap_to_status)?;
        let cmd = KvCommand::Cas {
            key: req.key,
            expected: req.expected_value,
            new_value: req.new_value,
            ttl_ns,
        };
        let resp = node.propose(cmd, mode).await.map_err(ggap_to_status)?;
        match resp {
            ggap_types::KvResponse::CasResult { success, current } => {
                Ok(Response::new(CasResponse {
                    header: Some(stub_header(self.node_id)),
                    success,
                    current: current.map(kv_entry_to_proto),
                }))
            }
            ggap_types::KvResponse::NoOp => unreachable!("CAS returned NoOp"),
            _ => Err(Status::internal("unexpected response variant")),
        }
    }

    type WatchStream = ReceiverStream<Result<WatchEvent, Status>>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let tx = self
            .watch_tx
            .as_ref()
            .ok_or_else(|| Status::unimplemented("watch not configured on this node"))?;
        let mut domain_rx = tx.subscribe();

        // Read the first message which must be a WatchCreateRequest.
        let mut inbound = request.into_inner();
        let create_req = match inbound.message().await? {
            Some(WatchRequest {
                request: Some(watch_request::Request::Create(c)),
            }) => c,
            _ => {
                return Err(Status::invalid_argument(
                    "first watch message must be WatchCreateRequest",
                ))
            }
        };

        let watch_id = NEXT_WATCH_ID.fetch_add(1, Ordering::Relaxed);
        let start_key = create_req.start_key;
        let end_key = create_req.end_key;
        let node_id = self.node_id;

        let (out_tx, out_rx) =
            tokio::sync::mpsc::channel::<Result<WatchEvent, Status>>(self.watch_output_buffer);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    // Listen for cancel requests from the client.
                    msg = inbound.message() => {
                        match msg {
                            Ok(Some(WatchRequest {
                                request: Some(watch_request::Request::Cancel(c)),
                            })) if c.watch_id == watch_id => {
                                // Client explicitly canceled this watch.
                                let _ = out_tx.send(Ok(WatchEvent {
                                    header: Some(stub_header(node_id)),
                                    r#type: EventType::Put as i32,
                                    kv: None,
                                    canceled: true,
                                    watch_id,
                                })).await;
                                break;
                            }
                            Ok(None) | Err(_) => {
                                // Client stream closed.
                                break;
                            }
                            _ => {
                                // Ignore unrecognized or mismatched messages.
                            }
                        }
                    }

                    // Receive broadcast events from the state machine.
                    result = domain_rx.recv() => {
                        match result {
                            Ok(evt) => {
                                // Filter by key range.
                                let in_range = (start_key.is_empty() || evt.key >= start_key)
                                    && (end_key.is_empty() || evt.key < end_key);
                                if !in_range {
                                    continue;
                                }

                                let event_type = match evt.kind {
                                    WatchEventKind::Put => EventType::Put as i32,
                                    WatchEventKind::Delete => EventType::Delete as i32,
                                    WatchEventKind::Expire => EventType::Expire as i32,
                                };
                                let kv = evt.entry.map(kv_entry_to_proto);
                                let proto_evt = WatchEvent {
                                    header: Some(stub_header(node_id)),
                                    r#type: event_type,
                                    kv,
                                    canceled: false,
                                    watch_id,
                                };
                                if out_tx.send(Ok(proto_evt)).await.is_err() {
                                    break; // client disconnected
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                // Fell behind the broadcast buffer — send canceled and close.
                                let _ = out_tx.send(Ok(WatchEvent {
                                    header: Some(stub_header(node_id)),
                                    r#type: EventType::Put as i32,
                                    kv: None,
                                    canceled: true,
                                    watch_id,
                                })).await;
                                break;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}
