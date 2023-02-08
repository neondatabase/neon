//! A simple log reloading mechanism, that uses [`LogReloadHandle`] received from `tracing` to toggle
//! different log levels for various pageserver logical parts.
//!
//! TODO Currently, not persistent and gets reset on pageserver restart, and allows to have
//! log filters for tenants and timelines, that do not exist on the pageserver.

use std::{
    collections::{hash_map, BTreeSet, HashMap},
    str::FromStr,
};

use anyhow::Context;

use pageserver_api::models::{ChangeLogLevelRequest, Scope};
use utils::{
    id::{TenantId, TimelineId},
    logging::{
        initial_env_filter, Directive, EnvFilter, Level, LogReloadHandle, DEFAULT_LOG_LEVEL,
    },
};

/// A way to update view, (re)load and reset custom pageserver log filters, applied dynamically.
///
/// TODO it would be nicer to wrap [`EnvFilter`] directly and modify/reload it, but
/// it has no `Clone` method to get the filter copy, neither there is any "view"
/// data struct for such filter in `tracing`.
///
/// Similarly, lower `update_filers` method could visit `EnvFilter` directly and adjust it, but there's no way
/// to publicly amend these, only replace and even inside, `Directive` has similar issues:
/// https://github.com/tokio-rs/tracing/blob/dd676608528847addf9187dd7e104955b563e550/tracing-subscriber/src/filter/env/directive.rs#L13
/// So the module uses strings and public builder API to construct new filters to replace the old ones.
///
/// Also, the filter applying logic might be improved:
/// https://github.com/tokio-rs/tracing/issues/2320
/// so we keep the wrapper for a while.
pub struct LogFilterManager {
    log_reload_handle: LogReloadHandle,
    directives: CustomFilterDirectives,
}

impl LogFilterManager {
    pub fn new(log_reload_handle: LogReloadHandle) -> Self {
        Self {
            log_reload_handle,
            directives: CustomFilterDirectives {
                general: BTreeSet::new(),
                tenant_timeline: HashMap::new(),
            },
        }
    }

    pub fn current_log_filter(&self) -> EnvFilter {
        self.log_reload_handle
            .with_current(|current_filter| current_filter.to_string())
            .expect("Failed to get handle's current filter")
            .parse()
            .expect("Filter failed to parse its own string representation")
    }

    /// Applies the filter update, adding a new one or removing an existing one from the filter directives.
    pub fn update_filter(&mut self, filter_update: ChangeLogLevelRequest) -> anyhow::Result<bool> {
        if self.directives.update(filter_update) {
            let new_filter = self.directives.create_filter();
            self.log_reload_handle
                .reload(new_filter)
                .context("Failed to reload log filters with new value: {new_filter}")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Removes all custom filter directives, falling back to the initial state.
    pub fn reset_log_filter(&mut self) -> anyhow::Result<()> {
        self.directives.clear();
        let new_filter = self.directives.create_filter();
        self.log_reload_handle
            .reload(new_filter)
            .context("Failed to reload log filters with new value: {new_filter}")
    }
}

/// A dynamic set of log filtering directives to use for global log filtering.
/// Global [`Directive`] pattern: `target[span{field1=value1,field2=value2}]=level`
/// is generated based on every directive added.
#[derive(Debug, Default)]
struct CustomFilterDirectives {
    /// E.g. `info`, `hyper=debug`, even `[{tenant=98d670ab7bee6f0051494306a1ab888f}]=error`,
    /// despite the possibility to add it in a better typed way.
    general: BTreeSet<Directive>,
    /// `[{tenant=98d670ab7bee6f0051494306a1ab888f}]=error` or `[{timeline=aa043653cb6f6189e17bab2fd0f30e1e}]=error`
    /// Multiple fields in the filter are not suppoered yet, see https://github.com/tokio-rs/tracing/issues/1584
    tenant_timeline: HashMap<TenantId, TenantFilterDirective>,
}

/// A tenant and related timeline log filters.
#[derive(Debug, Default)]
struct TenantFilterDirective {
    /// Tenant-wide log filter can be toggled independently of the timeline ones.
    /// If both enabled, the final log filter could look like
    /// `[{tenant=98d670ab7bee6f0051494306a1ab888f}]=error,[{timeline=aa043653cb6f6189e17bab2fd0f30e1e}]=debug,[{timeline=...`
    level: Option<Level>,
    timeline_directives: HashMap<TimelineId, Option<Level>>,
}

fn tenant_directive(tenant_id: TenantId, level: Level) -> Directive {
    Directive::from_str(&format!("[{{tenant={tenant_id}}}]={level}"))
        .expect("Tenant log filter directive should be correct")
}

fn timeline_directive(timeline_id: TimelineId, level: Level) -> Directive {
    Directive::from_str(&format!("[{{timeline={timeline_id}}}]={level}"))
        .expect("Timeline log filter directive should be correct")
}

impl CustomFilterDirectives {
    fn create_filter(&self) -> EnvFilter {
        let initial_env_filter = initial_env_filter();
        if self.is_empty() {
            return initial_env_filter;
        }

        let global_level = initial_env_filter
            .max_level_hint()
            .and_then(|level_filter| level_filter.into_level())
            .unwrap_or(DEFAULT_LOG_LEVEL);

        let mut env_filter = EnvFilter::builder()
            .with_regex(false)
            .with_default_directive(Directive::from(global_level))
            .parse("")
            .expect("Default env filter should be parsed");

        for log_override in self.general.clone() {
            env_filter = env_filter.add_directive(log_override)
        }

        for (&tenant_id, tenant_entry) in &self.tenant_timeline {
            if let Some(tenant_log_level) = tenant_entry.level {
                env_filter =
                    env_filter.add_directive(tenant_directive(tenant_id, tenant_log_level));
            }

            for (&timeline_id, &timeline_level) in &tenant_entry.timeline_directives {
                if let Some(timeline_level) = timeline_level {
                    env_filter =
                        env_filter.add_directive(timeline_directive(timeline_id, timeline_level));
                }
            }
        }

        env_filter
    }

    fn is_empty(&self) -> bool {
        self.general.is_empty() && self.tenant_timeline.is_empty()
    }

    fn clear(&mut self) {
        self.general.clear();
        self.tenant_timeline.clear();
    }

    fn update(&mut self, update: ChangeLogLevelRequest) -> bool {
        match update {
            ChangeLogLevelRequest::Custom { directive, enabled } => {
                if enabled {
                    self.general.insert(directive)
                } else {
                    self.general.remove(&directive)
                }
            }
            ChangeLogLevelRequest::Predefined { log_level, scope } => {
                let current_level = match scope {
                    Scope::Tenant { tenant_id } => {
                        &mut self.tenant_timeline.entry(tenant_id).or_default().level
                    }
                    Scope::Timeline {
                        tenant_id,
                        timeline_id,
                    } => self
                        .tenant_timeline
                        .entry(tenant_id)
                        .or_default()
                        .timeline_directives
                        .entry(timeline_id)
                        .or_default(),
                };

                let updated = *current_level != log_level;
                *current_level = log_level;
                self.clean_empty_entry(scope);

                updated
            }
        }
    }

    fn clean_empty_entry(&mut self, update_scope: Scope) {
        match update_scope {
            Scope::Tenant { tenant_id } => {
                if let hash_map::Entry::Occupied(mut tenant_o) =
                    self.tenant_timeline.entry(tenant_id)
                {
                    let tenant_entry = tenant_o.get_mut();
                    if tenant_entry.level.is_none() && tenant_entry.timeline_directives.is_empty() {
                        tenant_o.remove();
                    }
                }
            }
            Scope::Timeline {
                tenant_id,
                timeline_id,
            } => {
                if let hash_map::Entry::Occupied(mut tenant_o) =
                    self.tenant_timeline.entry(tenant_id)
                {
                    let tenant_entry = tenant_o.get_mut();

                    if let hash_map::Entry::Occupied(timeline_o) =
                        tenant_entry.timeline_directives.entry(timeline_id)
                    {
                        if timeline_o.get().is_none() {
                            timeline_o.remove();
                        }
                    }

                    if tenant_entry.level.is_none() && tenant_entry.timeline_directives.is_empty() {
                        tenant_o.remove();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_filter_additions_and_noop_general() {
        let mut filters = test_log_filters();
        let expected_filter = EnvFilter::try_new(DEFAULT_LOG_LEVEL.to_string())
            .unwrap()
            .to_string();

        assert_eq!(
            filters.current_log_filter().to_string(),
            expected_filter,
            "Freshly created LogFilters should not override the default level"
        );

        filters
            .update_filter(ChangeLogLevelRequest::Custom {
                directive: "pageserver=debug".parse().unwrap(),
                enabled: false,
            })
            .unwrap();
        for scope in [
            Scope::Tenant {
                tenant_id: TenantId::generate(),
            },
            Scope::Timeline {
                tenant_id: TenantId::generate(),
                timeline_id: TimelineId::generate(),
            },
        ] {
            filters
                .update_filter(ChangeLogLevelRequest::Predefined {
                    log_level: None,
                    scope,
                })
                .unwrap();
        }
        assert_eq!(
            filters.current_log_filter().to_string(),
            expected_filter,
            "LogFilters should not get udpated when new disabled custom filters or scoped ones without the log level are added"
        );
    }

    #[test]
    fn log_filter_custom_directive_additions_and_noop_readditions() {
        let mut filters = test_log_filters();
        let mut expected_filter = EnvFilter::try_new(DEFAULT_LOG_LEVEL.to_string()).unwrap();
        let new_directive: Directive = "pageserver=debug".parse().unwrap();

        filters
            .update_filter(ChangeLogLevelRequest::Custom {
                directive: new_directive.clone(),
                enabled: true,
            })
            .unwrap();
        expected_filter = expected_filter.add_directive(new_directive.clone());
        let updated_directives_string = expected_filter.to_string();
        assert_eq!(
            updated_directives_string,
            filters.current_log_filter().to_string(),
            "Adding a new enabled custom directive should add it to the filter list"
        );

        filters
            .update_filter(ChangeLogLevelRequest::Custom {
                directive: new_directive,
                enabled: true,
            })
            .unwrap();
        assert_eq!(
            updated_directives_string,
            filters.current_log_filter().to_string(),
            "Re-adding the same enabled filter is a noop"
        );
    }

    #[test]
    fn log_filter_scoped_additions_and_noop_readditions() {
        let mut filters = test_log_filters();
        let mut expected_filter = EnvFilter::try_new(DEFAULT_LOG_LEVEL.to_string()).unwrap();
        let level = Level::DEBUG;
        let tenant_id = TenantId::generate();

        filters
            .update_filter(ChangeLogLevelRequest::Predefined {
                log_level: Some(Level::DEBUG),
                scope: Scope::Tenant { tenant_id },
            })
            .unwrap();
        expected_filter = expected_filter.add_directive(tenant_directive(tenant_id, level));
        let updated_directives_string = expected_filter.to_string();
        assert_eq!(
            updated_directives_string,
            filters.current_log_filter().to_string(),
            "Adding a new enabled custom directive should add it to the filter list"
        );

        filters
            .update_filter(ChangeLogLevelRequest::Predefined {
                log_level: Some(Level::DEBUG),
                scope: Scope::Tenant { tenant_id },
            })
            .unwrap();
        assert_eq!(
            updated_directives_string,
            filters.current_log_filter().to_string(),
            "Re-adding the same enabled filter is a noop"
        );
    }

    #[test]
    fn log_filters_toggling() {
        let mut filters = test_log_filters();

        let directive_to_toggle: Directive = "hyper=warn".parse().unwrap();
        let other_directive: Directive = "pageserver=debug".parse().unwrap();
        let tenant_to_toggle = TenantId::generate();
        let other_tenant = TenantId::generate();
        let tenant_with_no_log_level = TenantId::generate();
        let timeline_to_toggle = TimelineId::generate();
        let other_timeline = TimelineId::generate();

        for request in [
            ChangeLogLevelRequest::Custom {
                directive: directive_to_toggle.clone(),
                enabled: true,
            },
            ChangeLogLevelRequest::Custom {
                directive: other_directive,
                enabled: true,
            },
            //
            ChangeLogLevelRequest::Predefined {
                log_level: Some(Level::DEBUG),
                scope: Scope::Tenant {
                    tenant_id: tenant_to_toggle,
                },
            },
            ChangeLogLevelRequest::Predefined {
                log_level: Some(Level::INFO),
                scope: Scope::Tenant {
                    tenant_id: other_tenant,
                },
            },
            //
            ChangeLogLevelRequest::Predefined {
                log_level: Some(Level::WARN), // timeline_to_toggle's log level
                scope: Scope::Timeline {
                    tenant_id: tenant_with_no_log_level,
                    timeline_id: timeline_to_toggle,
                },
            },
            ChangeLogLevelRequest::Predefined {
                log_level: Some(Level::WARN), // other_timeline's log level
                scope: Scope::Timeline {
                    tenant_id: tenant_with_no_log_level,
                    timeline_id: other_timeline,
                },
            },
        ] {
            let update_happened = filters.update_filter(request.clone()).unwrap_or_else(|e| {
                panic!("Standard log level filter {request:?} update caused an error: {e}")
            });
            assert!(
                update_happened,
                "Expected to enable the filter with request {request:?}"
            );
        }

        let filled_filters = filters.directives.create_filter().to_string();
        assert!(
            !filled_filters.contains(&tenant_with_no_log_level.to_string()),
            "'{filled_filters}' string should not contain filter for tenant {tenant_with_no_log_level} since it had no logs explicitly enabled"
        );
        assert!(
            filled_filters.contains(&tenant_to_toggle.to_string()),
            "'{filled_filters}' string should contain filter for tenant {tenant_to_toggle}"
        );
        assert!(
            filled_filters.contains(&other_tenant.to_string()),
            "'{filled_filters}' string should contain filter for tenant {other_tenant}"
        );
        assert!(
            filled_filters.contains(&timeline_to_toggle.to_string()),
            "'{filled_filters}' string should contain filter for timeline {timeline_to_toggle}"
        );
        assert!(
            filled_filters.contains(&other_timeline.to_string()),
            "'{filled_filters}' string should contain filter for timeline {other_timeline}"
        );

        for request in [
            ChangeLogLevelRequest::Custom {
                directive: directive_to_toggle,
                enabled: false,
            },
            ChangeLogLevelRequest::Predefined {
                log_level: None,
                scope: Scope::Tenant {
                    tenant_id: tenant_to_toggle,
                },
            },
            ChangeLogLevelRequest::Predefined {
                log_level: None,
                scope: Scope::Timeline {
                    tenant_id: tenant_with_no_log_level,
                    timeline_id: timeline_to_toggle,
                },
            },
        ] {
            let update_happened = filters.update_filter(request.clone()).unwrap_or_else(|e| {
                panic!("Standard log level filter {request:?} update caused an error: {e}")
            });
            assert!(
                update_happened,
                "Expected to disable the filter with request {request:?}"
            );
        }

        let toggled_filters = filters.directives.create_filter().to_string();
        assert!(
            !toggled_filters.contains(&tenant_with_no_log_level.to_string()),
            "'{filled_filters}' string should not contain filter for tenant {tenant_with_no_log_level} since it had no logs explicitly enabled"
        );
        assert!(
            !toggled_filters.contains(&tenant_to_toggle.to_string()),
            "'{filled_filters}' string should not have tenant {tenant_to_toggle} that got its log level toggled back"
        );
        assert!(
            toggled_filters.contains(&other_tenant.to_string()),
            "'{filled_filters}' string should still contain tenant {other_tenant} that was not affected by toggling"
        );
        assert!(
            !toggled_filters.contains(&timeline_to_toggle.to_string()),
            "'{filled_filters}' string should not contain timeline {timeline_to_toggle} that got its log level toggled back"
        );
        assert!(
            toggled_filters.contains(&other_timeline.to_string()),
            "'{filled_filters}' string should still contain timeline {other_timeline} that was not affected by toggling"
        );

        let default_log_filter = EnvFilter::try_new(DEFAULT_LOG_LEVEL.to_string())
            .unwrap()
            .to_string();
        assert_ne!(default_log_filter, filters.current_log_filter().to_string());
        filters.reset_log_filter().unwrap();
        assert_eq!(
            default_log_filter,
            filters.current_log_filter().to_string(),
            "Resetting the filters should result in the default resulting filter"
        );
    }

    fn test_log_filters() -> LogFilterManager {
        LogFilterManager::new(LogReloadHandle::noop())
    }
}
