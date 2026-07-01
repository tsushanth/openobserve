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

//! Shared-execution registry for Flight `do_get` fan-out.
//!
//! When the leader opens N parallel `do_get` streams to one follower (see the `RemoteScanExec`
//! fan-out), all N requests carry the same `job_id` and therefore the same follower trace_id.
//! They must run ONE physical execution and split its per-partition output streams among
//! themselves, otherwise the follower would re-scan N times. The first request builds the
//! execution; the rest await and take their contiguous bucket-group of streams. Session/slot
//! cleanup happens exactly once, when the last in-flight response finishes.
//!
//! EXPERIMENTAL: gated behind `ZO_FEATURE_FLIGHT_DOGET_FANOUT_ENABLED`. The lifecycle
//! (init race, backpressure across all N groups, cleanup timing) must be validated on a live
//! multi-node cluster before enabling in production. Known gap: a leader that crashes mid-query
//! can leave an entry whose remaining groups never arrive; a periodic age-based GC should evict
//! stale entries (TODO).

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use datafusion::{execution::SendableRecordBatchStream, prelude::SessionContext};
use flight::common::PreCustomMessage;
use parking_lot::Mutex;
use tonic::Status;

use crate::service::search::work_group::DeferredLock;

type SharedCell = Arc<tokio::sync::OnceCell<Arc<SharedExec>>>;

static REGISTRY: LazyLock<Mutex<HashMap<String, SharedCell>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A single follower execution whose per-partition output streams are handed out to the leader's
/// N parallel `do_get` requests.
pub struct SharedExec {
    /// Follower trace_id `{trace_id}-{job_id}`; doubles as the registry key.
    trace_id: String,
    /// One slot per output partition (bucket); each is taken exactly once by its owning request.
    streams: Vec<Mutex<Option<SendableRecordBatchStream>>>,
    /// Slots not yet claimed; when it reaches 0 the entry is evicted from the registry.
    remaining: AtomicUsize,
    /// Leader-facing custom messages (scan stats, metrics, peak memory, partial err). Taken by
    /// the `doget_index == 0` request so the leader aggregates them exactly once.
    custom_messages: Mutex<Option<Vec<PreCustomMessage>>>,
    /// Work-group lock (super cluster follower leader), held until all responses finish.
    lock: Mutex<Option<DeferredLock>>,
    /// Keeps the DataFusion session context alive while any response is still streaming.
    _ctx: SessionContext,
}

impl SharedExec {
    /// Take this request's contiguous bucket-group of streams. The last group absorbs any
    /// remainder when the bucket count does not divide evenly across the requests. Evicts the
    /// registry entry once every slot has been claimed.
    pub fn take_group(
        &self,
        doget_index: usize,
        doget_count: usize,
    ) -> Vec<SendableRecordBatchStream> {
        let (lo, hi) = group_range(self.streams.len(), doget_index, doget_count);
        let mut out = Vec::with_capacity(hi.saturating_sub(lo));
        for slot in &self.streams[lo..hi] {
            if let Some(s) = slot.lock().take() {
                out.push(s);
            }
        }
        // count slots this call actually claimed (not the range width), so a retried request
        // that finds its slots already taken doesn't underflow the counter
        let taken = out.len();
        if taken > 0 && self.remaining.fetch_sub(taken, Ordering::AcqRel) == taken {
            // all buckets claimed -> drop the registry handle. The `Arc<SharedExec>` lives on in
            // the in-flight responses and is freed (running cleanup) when the last one finishes.
            REGISTRY.lock().remove(&self.trace_id);
        }
        out
    }

    /// Custom messages for the leader; returns them once (for `doget_index == 0`) and an empty
    /// vec for every other request, so per-execution stats are reported exactly once.
    pub fn take_custom_messages(&self) -> Vec<PreCustomMessage> {
        self.custom_messages.lock().take().unwrap_or_default()
    }
}

impl Drop for SharedExec {
    fn drop(&mut self) {
        // Fires after every response holding an `Arc<SharedExec>` has finished, so the cleanup the
        // per-stream encoders skipped happens here exactly once.
        let trace_id = &self.trace_id;
        #[cfg(feature = "enterprise")]
        if o2_enterprise::enterprise::common::config::get_config()
            .work_group
            .max_nodes_per_query
            > 0
        {
            o2_enterprise::enterprise::search::admission::ledger::release(trace_id);
        }
        // defer_lock is only set for the super cluster follower leader
        if let Some(lock) = self.lock.lock().take() {
            drop(lock);
        } else {
            super::clear_session_data(trace_id);
        }
    }
}

/// Get the shared execution for `key`, building it once. The first caller runs `build`; the rest
/// await and share its result. On build error nothing is cached, so a later request may retry.
pub async fn get_or_build<F, Fut>(key: String, build: F) -> Result<Arc<SharedExec>, Status>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Arc<SharedExec>, Status>>,
{
    let cell = { REGISTRY.lock().entry(key).or_default().clone() };
    let shared = cell.get_or_try_init(build).await?;
    Ok(shared.clone())
}

/// Wrap a freshly executed plan's per-partition output streams into a `SharedExec`. `trace_id` is
/// both the follower trace_id and the registry key.
pub fn new_shared(
    trace_id: String,
    ctx: SessionContext,
    streams: Vec<SendableRecordBatchStream>,
    custom_messages: Vec<PreCustomMessage>,
    lock: Option<DeferredLock>,
) -> Arc<SharedExec> {
    let remaining = streams.len();
    Arc::new(SharedExec {
        trace_id,
        streams: streams.into_iter().map(|s| Mutex::new(Some(s))).collect(),
        remaining: AtomicUsize::new(remaining),
        custom_messages: Mutex::new(Some(custom_messages)),
        lock: Mutex::new(lock),
        _ctx: ctx,
    })
}

/// The contiguous slice of the `b` bucket streams that request `doget_index` of `doget_count`
/// owns. Groups partition `[0, b)` exactly; the last request absorbs any remainder so every
/// bucket is covered exactly once.
fn group_range(b: usize, doget_index: usize, doget_count: usize) -> (usize, usize) {
    let count = doget_count.max(1);
    let m = (b / count).max(1);
    let lo = (doget_index * m).min(b);
    // the last request takes everything left so no bucket is ever dropped
    let hi = if doget_index + 1 >= count {
        b
    } else {
        ((doget_index + 1) * m).min(b)
    };
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::group_range;

    #[test]
    fn test_group_range_even_split() {
        // 8 buckets across 2 requests => [0,4), [4,8)
        assert_eq!(group_range(8, 0, 2), (0, 4));
        assert_eq!(group_range(8, 1, 2), (4, 8));
    }

    #[test]
    fn test_group_range_last_absorbs_remainder() {
        // 8 buckets across 3 requests (m = 2): [0,2), [2,4), [4,8)
        assert_eq!(group_range(8, 0, 3), (0, 2));
        assert_eq!(group_range(8, 1, 3), (2, 4));
        assert_eq!(group_range(8, 2, 3), (4, 8));
    }

    #[test]
    fn test_group_range_covers_all_buckets_exactly_once() {
        // For every bucket count b and request count, the groups must partition [0, b):
        // no gaps, no overlaps.
        for b in 1..=16usize {
            for count in 1..=b {
                let mut covered = vec![false; b];
                for idx in 0..count {
                    let (lo, hi) = group_range(b, idx, count);
                    assert!(
                        lo <= hi && hi <= b,
                        "bad range b={b} count={count} idx={idx}"
                    );
                    for slot in covered.iter_mut().take(hi).skip(lo) {
                        assert!(!*slot, "overlap at b={b} count={count} idx={idx}");
                        *slot = true;
                    }
                }
                assert!(covered.iter().all(|&c| c), "gap at b={b} count={count}");
            }
        }
    }
}
