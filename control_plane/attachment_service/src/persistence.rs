/// The attachment service does not store most of its state durably.
///
/// The essential things to store durably are:
/// - generation numbers, as these must always advance monotonically to ensure data safety.
/// - PlacementPolicy and TenantConfig, as these are set externally.
struct Persistence {}

/// Parts of TenantState that are stored durably
struct TenantPersistence {
    pub(crate) generation: Generation,
    pub(crate) policy: PlacementPolicy,
    pub(crate) config: TenantConfig,
}
