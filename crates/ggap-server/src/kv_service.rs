use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ggap_consensus::{RaftNode, ShardRouter};
use ggap_proto::v1::{
    kv_service_server::KvService, watch_request, CasRequest, CasResponse, DeleteRequest,
    DeleteResponse, EventType, GetRequest, GetResponse, PutRequest, PutResponse, ScanRequest,
    ScanResponse, WatchEvent, WatchRequest,
};
use ggap_types::{DomainWatchEvent, KvCommand, KvEntry, ScanContinuation, WatchEventKind};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::convert::{
    ggap_to_status, kv_entry_to_proto, proto_read_consistency, proto_write_quorum, stub_header,
};
use crate::metrics::record;

/// Global monotonic counter for assigning unique watch IDs.
static NEXT_WATCH_ID: AtomicU64 = AtomicU64::new(1);

/// Maximum scan limit to prevent resource exhaustion from unbounded scans.
const MAX_SCAN_LIMIT: u32 = 10_000;

/// Maximum number of shards a single Scan RPC may hop across while looking
/// for matching keys. Bounds tail latency on sparse keyspaces while still
/// auto-skipping empty middle shards so clients don't pay one round-trip
/// per empty shard.
const MAX_SCAN_HOPS_PER_RPC: u32 = 8;

/// Returns the more restrictive non-empty exclusive end bound, or empty if
/// both are empty (unbounded).
fn tighter_end(a: &str, b: &str) -> String {
    match (a.is_empty(), b.is_empty()) {
        (true, _) => b.to_string(),
        (_, true) => a.to_string(),
        _ => a.min(b).to_string(),
    }
}

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

    async fn do_get(&self, req: GetRequest) -> Result<Response<GetResponse>, Status> {
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

    async fn do_put(&self, req: PutRequest) -> Result<Response<PutResponse>, Status> {
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

    async fn do_delete(&self, req: DeleteRequest) -> Result<Response<DeleteResponse>, Status> {
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

    async fn do_scan(&self, req: ScanRequest) -> Result<Response<ScanResponse>, Status> {
        // page_token is an opaque ScanContinuation. Empty ⇒ first page.
        let initial_start = if req.page_token.is_empty() {
            req.start_key.clone()
        } else {
            ScanContinuation::decode(&req.page_token)
                .map_err(ggap_to_status)?
                .next_key
        };
        let mode = proto_read_consistency(req.consistency);
        let limit = if req.limit == 0 {
            MAX_SCAN_LIMIT
        } else {
            req.limit.min(MAX_SCAN_LIMIT)
        };
        let req_end = req.end_key.clone();

        let mut all_kvs: Vec<KvEntry> = Vec::new();
        let mut cursor = initial_start;
        let mut remaining = limit;
        // None ⇒ caller has fully consumed the scan range. Some(blob) ⇒
        // more remains; client must pass it back.
        let mut next_token: Option<Vec<u8>> = None;

        for _hop in 0..MAX_SCAN_HOPS_PER_RPC {
            // Resolve the shard owning `cursor`. Routing by key always lands
            // on the correct shard even if a split committed since the token
            // was issued. Linearizable mode is enforced per shard inside
            // `node.scan`; cross-shard scans are NOT snapshot-isolated.
            let node = self
                .router
                .route_scan(&cursor)
                .await
                .map_err(ggap_to_status)?;
            let shard = self
                .router
                .shard_map()
                .lookup_shard(&cursor)
                .await
                .ok_or_else(|| {
                    Status::failed_precondition(format!("no shard for cursor '{cursor}'"))
                })?;
            let effective_end = tighter_end(&shard.range.end, &req_end);

            let (entries, continuation) = node
                .scan(&cursor, &effective_end, remaining, mode)
                .await
                .map_err(ggap_to_status)?;

            let got = entries.len() as u32;
            all_kvs.extend(entries);
            remaining = remaining.saturating_sub(got);

            if let Some(k) = continuation {
                // Limit hit inside this shard; resume here next RPC.
                next_token = Some(
                    ScanContinuation {
                        next_key: k,
                        shard_id_hint: shard.shard_id,
                    }
                    .encode()
                    .map_err(ggap_to_status)?,
                );
                break;
            }

            // Shard exhausted. Decide whether to stop or hop forward.
            let last_shard = shard.range.end.is_empty();
            let request_done =
                !req_end.is_empty() && (last_shard || shard.range.end.as_str() >= req_end.as_str());
            if last_shard || request_done || remaining == 0 {
                // Truly done; leave next_token as None.
                break;
            }

            // More shards remain in the requested range. Advance cursor to
            // this shard's end (= start of the next contiguous shard).
            cursor = shard.range.end.clone();

            if !all_kvs.is_empty() {
                // We already have data — surface it. Encode a token at the
                // boundary so the client picks up at the next shard.
                let hint = self
                    .router
                    .shard_map()
                    .lookup_shard(&cursor)
                    .await
                    .map(|s| s.shard_id)
                    .unwrap_or(shard.shard_id);
                next_token = Some(
                    ScanContinuation {
                        next_key: cursor.clone(),
                        shard_id_hint: hint,
                    }
                    .encode()
                    .map_err(ggap_to_status)?,
                );
                break;
            }
            // No results yet; auto-hop into the next shard (bounded by
            // MAX_SCAN_HOPS_PER_RPC) so a sparse keyspace doesn't force
            // the client into a token ping-pong of empty pages.
        }

        // If we exited the loop with no results AND no break-emitted token,
        // it means we exhausted MAX_SCAN_HOPS_PER_RPC. Surface progress so
        // the client can resume — otherwise scanning a sparse keyspace
        // larger than the hop budget would silently look "done".
        if next_token.is_none() && all_kvs.is_empty() && remaining == limit {
            // remaining == limit ⇒ we never returned even one entry.
            // Re-check whether cursor still has range left.
            let cursor_in_range = req_end.is_empty() || cursor.as_str() < req_end.as_str();
            if cursor_in_range
                && self
                    .router
                    .shard_map()
                    .lookup_shard(&cursor)
                    .await
                    .is_some()
            {
                let hint = self
                    .router
                    .shard_map()
                    .lookup_shard(&cursor)
                    .await
                    .map(|s| s.shard_id)
                    .unwrap_or(0);
                next_token = Some(
                    ScanContinuation {
                        next_key: cursor,
                        shard_id_hint: hint,
                    }
                    .encode()
                    .map_err(ggap_to_status)?,
                );
            }
        }

        let kvs = all_kvs.into_iter().map(kv_entry_to_proto).collect();
        let next_page_token = next_token.unwrap_or_default();

        Ok(Response::new(ScanResponse {
            header: Some(stub_header(self.node_id)),
            kvs,
            next_page_token,
        }))
    }

    async fn do_compare_and_swap(&self, req: CasRequest) -> Result<Response<CasResponse>, Status> {
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
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_get(request.into_inner()).await;
        record("ginnungagap.v1.KvService/Get", &result, start.elapsed());
        result
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_put(request.into_inner()).await;
        record("ginnungagap.v1.KvService/Put", &result, start.elapsed());
        result
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_delete(request.into_inner()).await;
        record("ginnungagap.v1.KvService/Delete", &result, start.elapsed());
        result
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_scan(request.into_inner()).await;
        record("ginnungagap.v1.KvService/Scan", &result, start.elapsed());
        result
    }

    async fn compare_and_swap(
        &self,
        request: Request<CasRequest>,
    ) -> Result<Response<CasResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_compare_and_swap(request.into_inner()).await;
        record(
            "ginnungagap.v1.KvService/CompareAndSwap",
            &result,
            start.elapsed(),
        );
        result
    }

    type WatchStream = ReceiverStream<Result<WatchEvent, Status>>;

    // Watch is intentionally uninstrumented: stream session lifetime is not
    // an RPC request duration (mixing would skew p99).
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
