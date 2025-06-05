use compute_api::responses::{LfcOffloadState, PromoteState};

use crate::compute::ComputeNode;

impl ComputeNode {
    pub async fn promote(&self) -> PromoteState {
        {
            let state = &mut self.state.lock().unwrap().promote_state;
            if let PromoteState::Promoting =
                std::mem::replace(state, PromoteState::Promoting)
            {
                return state;
            }
        }

        // reference:: configure
        // 1. Check if we're not primary
        // 4. Check we have safekeepers list supplied from primary
        // 2. Check we have prewarmed LFC
        // 3. Wait for last LSN to be committed
        // 4. Call pg_promote
        if !matches!(self.lfc_offload_state(), LfcOffloadState::Completed) {
        }
    }
}

