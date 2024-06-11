use std::{borrow::Cow, collections::HashMap, fmt::Debug, fmt::Display, sync::Arc};

use tokio::{sync::mpsc::error::TrySendError, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use utils::id::NodeId;

use crate::service::Service;

pub(crate) const MAX_RECONCILES_PER_OPERATION: usize = 10;

#[derive(Copy, Clone)]
pub(crate) struct Drain {
    node_id: NodeId,
}

#[derive(Copy, Clone)]
pub(crate) struct Fill {
    node_id: NodeId,
}

pub(crate) enum Operation {
    Drain(Drain),
    Fill(Fill),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OperationError {
    #[error("Operation precondition failed: {0}")]
    PreconditionFailed(Cow<'static, str>),
    #[error("Node state changed during operation: {0}")]
    NodeStateChanged(Cow<'static, str>),
    #[error("Operation cancelled")]
    Cancelled,
    #[error("Shutting down")]
    ShuttingDown,
}

struct OperationHandler {
    operation: Operation,
    #[allow(unused)]
    cancel: CancellationToken,
    #[allow(unused)]
    handle: JoinHandle<Result<(), OperationError>>,
}

#[derive(Default, Clone)]
struct OngoingOperations(Arc<std::sync::RwLock<HashMap<NodeId, OperationHandler>>>);

pub(crate) struct Controller {
    ongoing: OngoingOperations,
    service: Arc<Service>,
    sender: tokio::sync::mpsc::Sender<Operation>,
}

impl Controller {
    pub(crate) fn new(service: Arc<Service>) -> (Self, tokio::sync::mpsc::Receiver<Operation>) {
        let (operations_tx, operations_rx) = tokio::sync::mpsc::channel(1);
        (
            Self {
                ongoing: Default::default(),
                service,
                sender: operations_tx,
            },
            operations_rx,
        )
    }

    pub(crate) fn drain_node(&self, node_id: NodeId) -> Result<(), OperationError> {
        if let Some(handler) = self.ongoing.0.read().unwrap().get(&node_id) {
            return Err(OperationError::PreconditionFailed(
                format!(
                    "Background operation already ongoing for node: {}",
                    handler.operation
                )
                .into(),
            ));
        }

        self.sender.try_send(Operation::Drain(Drain { node_id }))?;

        Ok(())
    }

    pub(crate) fn fill_node(&self, node_id: NodeId) -> Result<(), OperationError> {
        if let Some(handler) = self.ongoing.0.read().unwrap().get(&node_id) {
            return Err(OperationError::PreconditionFailed(
                format!(
                    "Background operation already ongoing for node: {}",
                    handler.operation
                )
                .into(),
            ));
        }

        self.sender.try_send(Operation::Fill(Fill { node_id }))?;

        Ok(())
    }

    pub(crate) async fn handle_operations(
        &self,
        mut receiver: tokio::sync::mpsc::Receiver<Operation>,
    ) {
        while let Some(op) = receiver.recv().await {
            match op {
                Operation::Drain(drain) => self.handle_drain(drain),
                Operation::Fill(fill) => self.handle_fill(fill),
            }
        }
    }

    fn handle_drain(&self, _drain: Drain) {
        todo!("A later commit implements this stub");
    }

    fn handle_fill(&self, _fill: Fill) {
        todo!("A later commit implements this stub")
    }
}

impl<T> From<TrySendError<T>> for OperationError {
    fn from(value: TrySendError<T>) -> Self {
        match value {
            TrySendError::Full(_) => {
                Self::PreconditionFailed("Too many background operation in progress".into())
            }
            TrySendError::Closed(_) => Self::ShuttingDown,
        }
    }
}

impl Display for Drain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "drain {}", self.node_id)
    }
}

impl Display for Fill {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "fill {}", self.node_id)
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Operation::Drain(op) => write!(f, "{op}"),
            Operation::Fill(op) => write!(f, "{op}"),
        }
    }
}

impl Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "backround_node_operations::Controller")
    }
}
