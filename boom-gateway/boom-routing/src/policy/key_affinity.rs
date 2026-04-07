use boom_core::provider::Provider;
use dashmap::DashMap;
use std::sync::Arc;

use crate::inflight::InFlightTracker;
use super::SchedulePolicy;

/// Key-affinity scheduling: route requests from the same API key to the same
/// provider deployment for a given model, with load-aware rebalancing.
///
/// Affinity key: `{key_hash}:{model}` → `deployment_id`
///
/// Logic:
///   1. Single candidate → direct return
///   2. No key_hash provided → fall back to lowest-load
///   3. Below context_threshold → lowest-load (warm-up phase)
///   4. Affinity lookup → validate deployment still exists → rebalance check
///   5. First time or affinity miss → lowest-load → write affinity map
pub struct KeyAffinityPolicy {
    /// Reference to the in-flight tracker for load queries.
    tracker: Arc<InFlightTracker>,
    /// Affinity map: `{key_hash}:{model}` → `deployment_id`
    affinity: DashMap<String, String>,
    /// Context threshold: below this total input_chars across all providers,
    /// always pick lowest-load (warm-up to distribute initial load).
    /// 0 means always use affinity (no warm-up).
    context_threshold: u64,
    /// Rebalance threshold: if the preferred provider's load exceeds the
    /// minimum by more than this factor (absolute request count difference),
    /// reassign to the least loaded provider.
    rebalance_threshold: u64,
}

impl KeyAffinityPolicy {
    pub fn new(
        tracker: Arc<InFlightTracker>,
        context_threshold: u64,
        rebalance_threshold: u64,
    ) -> Self {
        Self {
            tracker,
            affinity: DashMap::new(),
            context_threshold,
            rebalance_threshold,
        }
    }
}

impl SchedulePolicy for KeyAffinityPolicy {
    fn select(
        &self,
        model: &str,
        candidates: &[Arc<dyn Provider>],
        key_hash: Option<&str>,
        _input_chars: u64,
    ) -> Option<Arc<dyn Provider>> {
        if candidates.is_empty() {
            return None;
        }
        if candidates.len() == 1 {
            return Some(candidates[0].clone());
        }

        // No key context → fall back to lowest-load.
        let key_hash = match key_hash {
            Some(k) => k,
            None => return select_lowest_load(&self.tracker, model, candidates),
        };

        let affinity_key = format!("{}:{}", key_hash, model);

        // Check if total in-flight is below context_threshold (warm-up).
        if self.context_threshold > 0 {
            let total_input: u64 = self
                .tracker
                .get_stats()
                .iter()
                .filter(|s| s.model.starts_with(model))
                .map(|s| s.inflight_input_chars)
                .sum();

            if total_input < self.context_threshold {
                // Warm-up: pick lowest-load and record affinity.
                let provider = select_lowest_load(&self.tracker, model, candidates);
                if let Some(ref p) = provider {
                    if let Some(did) = p.deployment_id() {
                        self.affinity.insert(affinity_key, did.to_string());
                    }
                }
                return provider;
            }
        }

        // Look up existing affinity.
        if let Some(preferred_id) = self.affinity.get(&affinity_key) {
            // Find the preferred provider in candidates.
            if let Some(provider) = candidates.iter().find(|c| {
                c.deployment_id()
                    .map(|id| id == preferred_id.as_str())
                    .unwrap_or(false)
            }) {
                // Rebalance check: if the preferred provider is significantly
                // more loaded than the least-loaded candidate, reassign.
                let load_preferred = load_for_deployment(&self.tracker, model, provider.as_ref());
                let (min_load, least_loaded) = min_load_candidate(&self.tracker, model, candidates);

                if load_preferred > min_load + self.rebalance_threshold {
                    // Rebalance to least loaded.
                    if let Some(did) = least_loaded.deployment_id() {
                        self.affinity.insert(affinity_key, did.to_string());
                    }
                    return Some(least_loaded);
                }

                return Some(provider.clone());
            }
            // Preferred deployment no longer in candidates — fall through.
        }

        // First time or affinity miss: pick lowest-load and record.
        let provider = select_lowest_load(&self.tracker, model, candidates);
        if let Some(ref p) = provider {
            if let Some(did) = p.deployment_id() {
                self.affinity.insert(affinity_key, did.to_string());
            }
        }
        provider
    }

    fn name(&self) -> &str {
        "key_affinity"
    }
}

/// Select the candidate with the fewest in-flight requests.
fn select_lowest_load(
    tracker: &InFlightTracker,
    model: &str,
    candidates: &[Arc<dyn Provider>],
) -> Option<Arc<dyn Provider>> {
    let (_min_load, provider) = min_load_candidate(tracker, model, candidates);
    Some(provider)
}

/// Find the candidate with the lowest in-flight request count.
fn min_load_candidate(
    tracker: &InFlightTracker,
    model: &str,
    candidates: &[Arc<dyn Provider>],
) -> (u64, Arc<dyn Provider>) {
    let stats = tracker.get_stats();
    let model_stats: std::collections::HashMap<&str, u64> = stats
        .iter()
        .filter(|s| s.model == model)
        .map(|s| (s.model.as_str(), s.inflight_requests))
        .collect();

    // Since InFlightTracker tracks by model name, not by deployment_id,
    // we need a different approach. We use a per-model load counter
    // indexed by deployment_id. For now, since the tracker aggregates
    // by model name, we distribute evenly as a tiebreaker.
    let _ = model_stats;

    // Use deployment_id to look up per-deployment stats if available,
    // otherwise fall back to even distribution.
    let stats_by_deployment = tracker.get_stats_by_deployment();
    let model_loads: std::collections::HashMap<&str, u64> = stats_by_deployment
        .iter()
        .filter(|s| s.model == model)
        .map(|s| (s.deployment_id.as_str(), s.inflight_requests))
        .collect();

    let mut best = candidates[0].clone();
    let mut best_load = u64::MAX;

    for candidate in candidates {
        let load = candidate
            .deployment_id()
            .and_then(|id| model_loads.get(id))
            .copied()
            .unwrap_or(0);

        if load < best_load {
            best_load = load;
            best = candidate.clone();
        }
    }

    (best_load, best)
}

/// Get the in-flight request count for a specific deployment.
fn load_for_deployment(
    tracker: &InFlightTracker,
    model: &str,
    provider: &dyn Provider,
) -> u64 {
    let deployment_id = match provider.deployment_id() {
        Some(id) => id,
        None => return 0,
    };

    tracker
        .get_stats_by_deployment()
        .iter()
        .filter(|s| s.model == model && s.deployment_id == deployment_id)
        .map(|s| s.inflight_requests)
        .sum()
}
