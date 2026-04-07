pub mod round_robin;

use boom_core::provider::Provider;
use std::sync::Arc;

/// Trait for scheduling policies. Each policy decides which provider
/// to select from a list of candidates for a given model.
pub trait SchedulePolicy: Send + Sync {
    /// Select one provider from the candidates list for the given model.
    fn select(&self, model: &str, candidates: &[Arc<dyn Provider>]) -> Option<Arc<dyn Provider>>;

    /// Policy name (for display / config validation).
    fn name(&self) -> &str;
}
