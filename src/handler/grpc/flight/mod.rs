// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{io::Cursor, sync::Arc};

use arrow::ipc::{CompressionType, writer::IpcWriteOptions};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use config::{
    cluster::LOCAL_NODE, datafusion::request::FlightSearchRequest, meta::search::ScanStats,
};
use datafusion::{
    common::{DataFusionError, Result},
    physical_plan::execute_stream_partitioned,
};
use flight::common::{MetricsInfo, PreCustomMessage};
use futures::{StreamExt, stream::BoxStream};
use futures_util::pin_mut;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
#[cfg(feature = "enterprise")]
use {
    crate::service::search::SEARCH_SERVER,
    o2_enterprise::enterprise::{common::config::get_config as get_o2_config, search::TaskStatus},
};

use crate::{
    handler::grpc::{
        MetadataMap,
        flight::{
            stream::FlightEncoderStreamBuilder,
            visitor::{
                get_cluster_metrics, get_partial_err, get_peak_memory, get_peak_memory_from_ctx,
                get_scan_stats,
            },
        },
    },
    service::search::{
        grpc::flight as grpcFlight,
        inspector::{SearchInspectorFieldsBuilder, search_inspector_fields},
        work_group::DeferredLock,
    },
};

mod doget_registry;
mod partition_encoder;
mod stream;
pub mod visitor;

#[derive(Default)]
pub struct FlightServiceImpl;

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let _start = std::time::Instant::now();
        let cfg = config::get_config();

        let parent_cx = opentelemetry::global::get_text_map_propagator(|prop| {
            prop.extract(&MetadataMap(request.metadata()))
        });
        let span = tracing::info_span!("grpc:search:flight:do_get");
        let _ = span.set_parent(parent_cx);

        // decode ticket to RemoteExecNode
        let ticket = request.into_inner();
        let mut buf = Cursor::new(ticket.ticket);
        let req = proto::cluster_rpc::FlightSearchRequest::decode(&mut buf)
            .map_err(|e| DataFusionError::Internal(format!("{e:?}")))
            .map_err(|e| Status::internal(e.to_string()))?;

        let req: FlightSearchRequest = req.into();
        let trace_id = format!(
            "{}-{}",
            req.query_identifier.trace_id, req.query_identifier.job_id
        );
        let is_super_cluster = req.super_cluster_info.is_super_cluster;
        let timeout = req.search_info.timeout as u64;
        log::info!("[trace_id {trace_id}] flight->search: do_get, timeout: {timeout}s",);

        // do_get fan-out: the leader opens several parallel streams to this follower, all sharing
        // one job_id (hence one trace_id). They share a single execution and split its per-bucket
        // output streams; the first request builds it, the rest take their group.
        //
        // ZO_FEATURE_FLIGHT_DOGET_FANOUT_ENABLED gates the LEADER's decision to fan out; a
        // follower must honor doget_count > 1 unconditionally. Checking the local flag here
        // would, on config drift, send every fan-out request down the legacy path — N full
        // executions each returning ALL buckets, silently multiplying the leader's data by N.
        let doget_count = req.query_identifier.doget_count as usize;
        let doget_index = req.query_identifier.doget_index as usize;
        if doget_count > 1 {
            let shared = doget_registry::get_or_build(trace_id.clone(), {
                let trace_id = trace_id.clone();
                let req = req.clone();
                let span = span.clone();
                move || build_shared_exec(trace_id, req, span)
            })
            .await?;

            // duplicate/retried requests get an error here instead of a silent empty stream
            let my_streams = shared.take_group(doget_index, doget_count)?;
            // per-execution stats/metrics are reported exactly once: the first request to reach
            // this point takes them, so they survive even if a sibling request never arrives
            let custom_messages = shared.take_custom_messages();

            let start = std::time::Instant::now();
            let write_options: IpcWriteOptions = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut encoder = FlightEncoderStreamBuilder::new(write_options, 33554432)
                .with_trace_id(trace_id.to_string())
                .with_is_super(is_super_cluster)
                // the shared execution owns session/slot cleanup for all its streams; the guard
                // delays that cleanup until this response's encoder tasks have fully stopped
                .with_skip_session_cleanup(true)
                .with_cleanup_guard(shared.clone())
                .with_start(start)
                .with_custom_messages(custom_messages)
                .build(my_streams, span);

            let stream = async_stream::stream! {
                // keep the shared execution alive until this response finishes; the last response
                // to drop its handle runs the one-time session/slot cleanup
                let _shared = shared;
                let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(timeout));
                pin_mut!(timeout);
                loop {
                    tokio::select! {
                        batch = encoder.next() => {
                            if let Some(batch) = batch {
                                yield batch
                            } else {
                                break;
                            }
                        }
                        _ = &mut timeout => {
                            log::info!("[trace_id {trace_id}] flight->search: timeout");
                            break;
                        }
                    }
                }
            };
            return Ok(Response::new(Box::pin(stream) as Self::DoGetStream));
        }

        // Note: all async should in this place, otherwise it will break tracing
        // https://docs.rs/tracing/latest/tracing/span/struct.Span.html#in-asynchronous-code
        let req_move = req.clone();
        let trace_id_move = trace_id.clone();
        let result = async move {
            #[cfg(feature = "enterprise")]
            if is_super_cluster && !SEARCH_SERVER.contain_key(&trace_id_move).await {
                // this is for work_group check in super cluster follower leader
                SEARCH_SERVER
                    .insert(
                        trace_id_move.clone(),
                        TaskStatus::new_follower(vec![], false),
                    )
                    .await;
            }

            let result = get_ctx_and_physical_plan(&trace_id_move, &req_move).await;

            #[cfg(feature = "enterprise")]
            if is_super_cluster && !SEARCH_SERVER.is_leader(&trace_id_move).await {
                // this is for work_group check in super cluster follower leader
                SEARCH_SERVER.remove(&trace_id_move, false).await;
            }

            result
        }
        .instrument(span.clone())
        .await;

        log::info!(
            "{}",
            search_inspector_fields(
                format!(
                    "[trace_id {trace_id}] flight->do_get: get_ctx_and_physical_plan took: {} ms",
                    _start.elapsed().as_millis(),
                ),
                SearchInspectorFieldsBuilder::new()
                    .trace_id(trace_id.to_string())
                    .node_name(LOCAL_NODE.name.clone())
                    .component("flight::do_get get_ctx_and_physical_plan".to_string())
                    .search_role("follower".to_string())
                    .duration(_start.elapsed().as_millis() as usize)
                    .build()
            )
        );

        // prepare dataufion context
        let (ctx, plan, lock, scan_stats) = match result {
            Ok(v) => v,
            Err(e) => {
                clear_session_data(&trace_id);
                #[cfg(feature = "enterprise")]
                if get_o2_config().work_group.max_nodes_per_query > 0 {
                    o2_enterprise::enterprise::search::admission::ledger::release(&trace_id);
                    log::error!(
                        "[trace_id {trace_id}] flight->search: do_get physical plan generate error: {e:?}",
                    );
                }
                return Err(Status::internal(e.to_string()));
            }
        };

        log::info!(
            "[trace_id {trace_id}] flight->search: executing stream, is super cluster: {is_super_cluster}"
        );

        if cfg.common.print_key_sql {
            log::info!(
                "[trace_id {trace_id}] follow physical plan, is_super_cluster_follower_leader: {is_super_cluster}"
            );
            log::info!(
                "{}",
                config::meta::plan::generate_plan_string(&trace_id, plan.as_ref())
            );
        }

        let start = std::time::Instant::now();
        let write_options: IpcWriteOptions = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .map_err(|e| {
                // clear session data
                clear_session_data(&trace_id);
                log::error!(
                    "[trace_id {trace_id}] flight->search: do_get create IPC write options error: {e:?}",
                );
                Status::internal(e.to_string())
            })?;

        // per-execution stats/metrics/partial-err handed to the leader as custom messages
        let custom_messages = collect_custom_messages(
            &ctx,
            &plan,
            scan_stats,
            is_super_cluster,
            req.search_info.is_analyze,
        );

        // One stream per output partition so they encode in parallel
        let streams =
            execute_stream_partitioned(plan, ctx.task_ctx()).map_err(|e| {
                clear_session_data(&trace_id);
                #[cfg(feature = "enterprise")]
                if get_o2_config().work_group.max_nodes_per_query > 0 {
                    o2_enterprise::enterprise::search::admission::ledger::release(&trace_id);
                    log::error!(
                        "[trace_id {trace_id}] flight->search: do_get physical plan execution error: {e:?}",
                    );
                }
                Status::internal(e.to_string())
            })?;

        let mut stream = FlightEncoderStreamBuilder::new(write_options, 33554432)
            .with_trace_id(trace_id.to_string())
            .with_is_super(is_super_cluster)
            .with_defer_lock(lock)
            .with_start(start)
            .with_custom_messages(custom_messages)
            .build(streams, span);

        let stream = async_stream::stream! {
            let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(timeout));
            pin_mut!(timeout);
            loop {
                tokio::select! {
                    batch = stream.next() => {
                        if let Some(batch) = batch {
                            yield batch
                        } else {
                            break;
                        }
                    }
                    _ = &mut timeout => {
                        log::info!("[trace_id {trace_id}] flight->search: timeout");
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

type PlanResult = (
    datafusion::prelude::SessionContext,
    Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    Option<DeferredLock>,
    ScanStats,
);

#[cfg(feature = "enterprise")]
#[tracing::instrument(name = "service:search:grpc:flight::enter", skip_all)]
async fn get_ctx_and_physical_plan(
    trace_id: &str,
    req: &FlightSearchRequest,
) -> Result<PlanResult, infra::errors::Error> {
    if req.super_cluster_info.is_super_cluster {
        let (ctx, physical_plan, lock, scan_stats) =
            crate::service::search::super_cluster::follower::search(trace_id, req).await?;
        Ok((ctx, physical_plan, Some(lock), scan_stats))
    } else {
        let (ctx, physical_plan, scan_stats) = grpcFlight::search(trace_id, req).await?;
        Ok((ctx, physical_plan, None, scan_stats))
    }
}

#[cfg(not(feature = "enterprise"))]
#[tracing::instrument(name = "service:search:grpc:flight::enter", skip_all)]
async fn get_ctx_and_physical_plan(
    trace_id: &str,
    req: &FlightSearchRequest,
) -> Result<PlanResult, infra::errors::Error> {
    let (ctx, physical_plan, scan_stats) = grpcFlight::search(trace_id, req).await?;
    Ok((ctx, physical_plan, None, scan_stats))
}

/// Build one shared follower execution for the do_get fan-out: prepare the context and physical
/// plan, execute it into per-partition streams, and gather the leader-facing custom messages.
/// Runs exactly once per (trace_id, job_id); the parallel do_get requests share the result.
async fn build_shared_exec(
    trace_id: String,
    req: FlightSearchRequest,
    span: tracing::Span,
) -> Result<Arc<doget_registry::SharedExec>, Status> {
    let is_super_cluster = req.super_cluster_info.is_super_cluster;

    let result = async {
        #[cfg(feature = "enterprise")]
        if is_super_cluster && !SEARCH_SERVER.contain_key(&trace_id).await {
            SEARCH_SERVER
                .insert(trace_id.clone(), TaskStatus::new_follower(vec![], false))
                .await;
        }
        let result = get_ctx_and_physical_plan(&trace_id, &req).await;
        #[cfg(feature = "enterprise")]
        if is_super_cluster && !SEARCH_SERVER.is_leader(&trace_id).await {
            SEARCH_SERVER.remove(&trace_id, false).await;
        }
        result
    }
    .instrument(span.clone())
    .await;

    let (ctx, physical_plan, lock, scan_stats) = match result {
        Ok(v) => v,
        Err(e) => {
            clear_session_data(&trace_id);
            #[cfg(feature = "enterprise")]
            if get_o2_config().work_group.max_nodes_per_query > 0 {
                o2_enterprise::enterprise::search::admission::ledger::release(&trace_id);
            }
            return Err(Status::internal(e.to_string()));
        }
    };

    let custom_messages = collect_custom_messages(
        &ctx,
        &physical_plan,
        scan_stats,
        is_super_cluster,
        req.search_info.is_analyze,
    );

    // one stream per output partition (bucket) so they encode in parallel
    let streams =
        execute_stream_partitioned(physical_plan, ctx.task_ctx().clone()).map_err(|e| {
            clear_session_data(&trace_id);
            #[cfg(feature = "enterprise")]
            if get_o2_config().work_group.max_nodes_per_query > 0 {
                o2_enterprise::enterprise::search::admission::ledger::release(&trace_id);
            }
            Status::internal(e.to_string())
        })?;

    Ok(doget_registry::new_shared(
        trace_id,
        ctx,
        streams,
        custom_messages,
        lock,
    ))
}

/// The custom messages appended to a do_get response: per-execution scan stats, EXPLAIN ANALYZE
/// metrics, peak memory, and partial-error refs. Shared by the single-stream path and the
/// fan-out shared execution so the leader receives the same set either way.
fn collect_custom_messages(
    ctx: &datafusion::prelude::SessionContext,
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    scan_stats: ScanStats,
    is_super_cluster: bool,
    is_analyze: bool,
) -> Vec<PreCustomMessage> {
    // used for EXPLAIN ANALYZE to collect metrics after stream is done
    let metrics = is_analyze.then_some(MetricsInfo {
        plan: plan.clone(),
        is_super_cluster,
        func: Box::new(super_cluster_enabled),
    });
    // peak memory is read after stream execution, so it travels via shared references
    let peak_memory = get_peak_memory_from_ctx(ctx);
    // used for super cluster follower leader to get information from follower node
    let scan_stats_ref = get_scan_stats(plan);
    let metrics_ref = get_cluster_metrics(plan);
    let peak_memory_ref = get_peak_memory(plan);
    let partial_err_ref = get_partial_err(plan);
    vec![
        PreCustomMessage::ScanStats(scan_stats),
        PreCustomMessage::ScanStatsRef(scan_stats_ref),
        PreCustomMessage::Metrics(metrics),
        PreCustomMessage::MetricsRef(metrics_ref),
        PreCustomMessage::PeakMemoryRef(Some(peak_memory)),
        PreCustomMessage::PeakMemoryRef(peak_memory_ref),
        PreCustomMessage::PartialErrRefEarly(partial_err_ref.clone()),
        PreCustomMessage::PartialErrRef(partial_err_ref),
    ]
}

fn clear_session_data(trace_id: &str) {
    // clear session data
    crate::service::search::datafusion::storage::file_list::clear(trace_id);
    // release wal lock files
    crate::common::infra::wal::release_request(trace_id);
    log::info!("Cleared session for trace_id: {trace_id}");
}

fn super_cluster_enabled() -> bool {
    #[cfg(feature = "enterprise")]
    if get_o2_config().super_cluster.enabled {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_service_impl_default() {
        let _impl = FlightServiceImpl::default();
    }

    #[test]
    fn test_super_cluster_disabled_in_oss() {
        assert!(!super_cluster_enabled());
    }
}
