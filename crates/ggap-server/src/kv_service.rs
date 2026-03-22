use std::sync::Arc;

use ggap_consensus::{RaftNode, ShardRouter};
use ggap_proto::v1::{
    kv_service_server::KvService, CasRequest, CasResponse, DeleteRequest, DeleteResponse,
    GetRequest, GetResponse, PutRequest, PutResponse, ScanRequest, ScanResponse, WatchEvent,
    WatchRequest,
};
use ggap_types::KvCommand;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::convert::{
    ggap_to_status, kv_entry_to_proto, proto_read_consistency, proto_write_quorum, stub_header,
};

pub struct KvServiceImpl {
    router: Arc<ShardRouter>,
    node_id: u64,
}

impl KvServiceImpl {
    pub fn new(router: Arc<ShardRouter>, node_id: u64) -> Self {
        KvServiceImpl { router, node_id }
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
        let node = self.router.route_read(&req.key).await.map_err(ggap_to_status)?;
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
        let mode = proto_write_quorum(req.quorum);
        let ttl_ns = if req.ttl_secs == 0 {
            None
        } else {
            Some(req.ttl_secs as i64 * 1_000_000_000)
        };
        let node = self.router.route_write(&req.key).await.map_err(ggap_to_status)?;
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
        let node = self.router.route_write(&req.key).await.map_err(ggap_to_status)?;
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
        let node = self
            .router
            .route_scan(&start_key, &req.end_key)
            .await
            .map_err(ggap_to_status)?;
        let (entries, continuation) = node
            .scan(&start_key, &req.end_key, req.limit, mode)
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
        let mode = proto_write_quorum(req.quorum);
        let ttl_ns = if req.ttl_secs == 0 {
            None
        } else {
            Some(req.ttl_secs as i64 * 1_000_000_000)
        };
        let node = self.router.route_write(&req.key).await.map_err(ggap_to_status)?;
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

    // Watch is Phase 5 — return unimplemented immediately.
    type WatchStream = ReceiverStream<Result<WatchEvent, Status>>;

    async fn watch(
        &self,
        _request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch not implemented until Phase 5"))
    }
}
