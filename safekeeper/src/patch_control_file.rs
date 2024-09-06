use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::{state::TimelinePersistentState, timeline::Timeline};

#[derive(Deserialize, Debug, Clone)]
pub struct Request {
    /// JSON object with fields to update
    pub updates: serde_json::Value,
    /// List of fields to apply
    pub apply_fields: Vec<String>,
}

#[derive(Serialize)]
pub struct Response {
    pub old_control_file: TimelinePersistentState,
    pub new_control_file: TimelinePersistentState,
}

/// Patch control file with given request. Will update the persistent state using
/// fields from the request and persist the new state on disk.
pub async fn handle_request(tli: Arc<Timeline>, request: Request) -> anyhow::Result<Response> {
    let response = tli
        .map_control_file(|state| {
            let old_control_file = state.clone();
            let new_control_file = state_apply_diff(&old_control_file, &request)?;

            info!(
                "patching control file, old: {:?}, new: {:?}, patch: {:?}",
                old_control_file, new_control_file, request
            );
            *state = new_control_file.clone();

            Ok(Response {
                old_control_file,
                new_control_file,
            })
        })
        .await?;

    Ok(response)
}

fn state_apply_diff(
    state: &TimelinePersistentState,
    request: &Request,
) -> anyhow::Result<TimelinePersistentState> {
    let mut json_value = serde_json::to_value(state)?;

    if let Value::Object(a) = &mut json_value {
        if let Value::Object(b) = &request.updates {
            json_apply_diff(a, b, &request.apply_fields)?;
        } else {
            anyhow::bail!("request.updates is not a json object")
        }
    } else {
        anyhow::bail!("TimelinePersistentState is not a json object")
    }

    let new_state: TimelinePersistentState = serde_json::from_value(json_value)?;
    Ok(new_state)
}

fn json_apply_diff(
    object: &mut serde_json::Map<String, Value>,
    updates: &serde_json::Map<String, Value>,
    apply_keys: &Vec<String>,
) -> anyhow::Result<()> {
    for key in apply_keys {
        if let Some(new_value) = updates.get(key) {
            if let Some(existing_value) = object.get_mut(key) {
                *existing_value = new_value.clone();
            } else {
                anyhow::bail!("key not found in original object: {}", key);
            }
        } else {
            anyhow::bail!("key not found in request.updates: {}", key);
        }
    }

    Ok(())
}
