# Improving runtime by making use of the RAII idiom

## Motivation

Currently, we have several problems regarding the runtime:

- `TenantState` doesn't clearly show what happens with the tenant. Repository and code comments need to describe how state transitions occur and what they mean. It is critical since the code is going to have more states. The same applies to `TimelineState`.
- Current tenant and timeline access, shutdown design fails to shut down tenants properly, timelines, background tasks, and control their lifetimes, resulting in races.
- `tasks_mgr` runtime is redundant, and its design allows us to have even more races without noticing.

This RFC describes how to rewrite a runtime in a more RAII fashion, fixing part of its problems.

## Small quality of life changes

Before we start, some synchronisation primitives should be proposed.

1. In our code, there're multiple places where we use `tokio::watch::{Sender, Receiver}` with type `()`. It's hard to track down whether we're just waiting for the drop, for a signal or both from the sender. To make it clear, a couple of wrappers are needed:
    - `SignalSender` / `SignalWatcher`: wait only for proper signals and `panic!` when it receives `RecvError`.
    - `DropSender` / `DropWatcher`: wait only for `RecvError` and don't have any function in the interface to signal.
    - Non-generic `Sender` / `Receiver` wrappers for both drops and signals.
2. For the cancellation proposal, we'll need an object that both can wait for a task to finish and send a cancellation request. We'll call it `CancelationSender` / `CancellationReceiver`. It could be coded using two tokio channels.
3. For the cancellation proposal, we'll need a triple `StateKeeper` / `StateSubscriber` / `StateHider`. This triple atomically changes the state and sends it to subscribers like `tokio::watch`. But when there's at least one `StateHider`, we put the new state to the buffer that is being read only by subscribers, and `StateKeeper` will read the older value. After all `StateHider`s are gone, we atomically (with respect to the new possible `StateHider`s) change the state.

    ```rust
    impl<S> StateKeeper<S> {
        pub fn get_state(&self) -> S;
        pub fn set_state(&self, state: S);
        pub fn subscribe(&self) -> StateSubscriber;
    }

    impl<S> StateSubscriber<S> {
        pub fn get_state(&self) -> S;
        pub fn state_hider(&self) -> StateHider;
    }

    impl<S> Clone for StateSubscriber<S>;

    impl<S> Drop for StateHider<S>;
    ```

 The purpose of this non-trivial primitive is to hide the state changes from the closures running in the tenant and timeline until closures finish the work. It is explained in more detail later.

## States and transitions in `TenantState`

Currently, we have 3 tenant states:

- `Active { background_jobs_running: bool }` - fully operational, _its background jobs might be running or not_.
- `Paused` - is recognised by the pageserver but not yet ready to operate, e.g. not present locally and being downloaded or being read into memory from the file system.
- `Broken` - is recognised by the pageserver but no longer used for any operations, as it failed to get activated.

Problems:

- It needs to be clarified how to know that tenant is being dropped.
- `Active` state has an option that's not needed. If we do not want to run background tasks, we may wish to have an alternative to the `Active` state without running background jobs.
- Actually, we can change the state from `Paused` to `Active` _and vice versa_! Engineers must remember that when writing the code, the only reason to get to know that is to read the code. Only some of the people in the team know about that.

Proposed states:

1. `Infant` - just created, no activity yet.
    - Do we need this state?
2. `Loading` - currently loads its data from the disk to the memory.
3. `Broken` - cannot load the tenant; unrecoverable error happened. It is the final state.
4. `Downloading` - downloading the data files from object storage.
    - Is it better to change to `Attaching`?
5. `Active` - fully operational, background tasks are running.
6. `ShuttingDown` - tenant is being shut down; no new closures could be run.
    - What should happen to operations running on the tenant when we change the state to this one?
    - Is it better to change to `Terminating`?
7. `Shutdown` - tenant is shut down, background operations are finished, and ready to recycle. It is the final state.
    - Is it better to change to `Terminated`?

Transitions:

1. `Infant` -> `Loading`, `Downloading`.
2. `Loading` -> `Active`, `Broken`.
3. `Broken` is the final state.
4. `Downloading` -> `Active`, `Broken`.
5. `Active` -> `ShuttingDown`.
6. `ShuttingDown` -> `Shutdown`.
7. `Shutdown` is the final state.

The transition graph has no cycles, which is a good property.

Generally, we have 3 subsets of states:

1. Paused - tries to load itself: `Loading`, `Downloading`.
2. Alive - can operate: `Infant`, `Active`.
3. Dead - cannot launch any operations, possibly already dropped: `Broken`, `ShuttingDown`, `Shutdown`.

## `TenantAccessor`

The first part of the chapter will be about `TenantAccessor` specifically. Later, we'll discuss `TimelineAccessor`.

### Top-level interface

We need a proper tree-like structure with a bare minimum of global states to use RAII, unlike carefully using `shutdown_tasks` as we do now.

The idea is to have the following tenant API:

```rust
/// Gets the alive tenant from the in-memory data, then applies the specified
/// function. Returns error if there is no tenant in memory or the tenant is not
/// ready to launch the closure.
pub fn with_tenant<F, O>(tenant_id: TenantId, func: F) -> anyhow::Result<O>
where
    F: FnOnce(TenantSyncRef) -> O {}

/// Gets the alive tenant from the in-memory data, then applies the specified
/// asynchronous function. Returns error if there is no tenant in memory or
/// tenant is not ready to launch the closure.
pub async fn with_tenant_async<F, T, O>(tenant_id: TenantId, func: F) -> anyhow::Result<O>
where
    F: FnOnce(TenantAsyncRef) -> T,
    T: Future<Output = O> {}
```

So, _we won't have access to the tenant directly_. Instead, we only use transaction-inspired API with some closure that can access the tenant. These functions will be referred to as closures.

- `TenantSyncRef` is `&Tenant`, but with a limited interface not to allow simple mistakes like waiting for something inside.
- `TenantAsyncRef` is `Arc<Tenant>` only because of current Rust's limitations of async closures. Later, it will also be a `&Tenant`. It also has a limited interface to prevent some kinds of mistakes.

_These closures **are not supposed to run any long-running code**, and should be checked on code reviews._

To solve a problem, we'll have a global private mapping from `TenantId` to `TenantAccessor`, accessible only by `with_tenant`. `TenantAccessor` is a kind of guard similar to `Arc` in Rust but not the same. It allows running the closures with the tenant in scope but won't run them if it's not in the `Active` state, and this accessor is responsible for all cancellations.

The interface:

```rust
impl TenantAccessor {
    /// Creates a new accessor. Check the state is not `TenantState::Infant`.
    /// Spawns a closure in tokio runtime to wait for the state to become dead.
    /// Then, the tenant is shut down.
    pub fn new(tenant: Tenant) -> Self;

    /// Subscribe for the tenant drop. If the tenant is already dropped, then
    /// watcher will also reflect that.
    pub fn subscribe_for_shutdown(&self) -> DropWatcher;

    /// Applies the specified function to the tenant. Returns error if the tenant is
    /// not ready to launch the closure.
    pub fn with_tenant<F, O>(&self, func: F) -> anyhow::Result<O>
    where
    F: FnOnce(TenantSyncRef) -> O {}

    /// Applies the specified function to the tenant. Returns error if the tenant is
    /// not ready to launch the closure.
    pub async fn with_tenant_async<F, T, O>(&self, func: F) -> anyhow::Result<O>
    where
    F: FnOnce(TenantAsyncRef) -> T,
    T: Future<Output = O> {}
}

impl Drop for TenantAccessor {
    // schedule a drop of itself in tokio runtime with necessary joins...
}
```

### `TenantAccessor` and states

This is how the accessor behaves when it sees different states:

1. `Infant` - we cannot create a guard around the tenant at this stage. We should start loading some layers and become `Loading` or `Downloading` first and only after creating an accessor.
2. `Loading` - we cannot run any operations on the tenant until it is either in an `Active` or `Broken` state.
3. `Broken` - we cannot run any operations on the tenant.
4. `Downloading` - we cannot run any operations on the tenant until it is either in an `Active` or `Broken` state.
5. `Active` - the tenant can run any closures. **That means it's the only way to run any closures on tenants**.
6. `ShuttingDown` - we cannot run any operations on the tenant.
7. `Shutdown` - we cannot run any operations on the tenant.

### `TenantAccessor` and cancelations

One great question is: what guarantees do we have about tenants inside the closures we're running?

After we introduced the `with_tenant` functions, there are a bunch of closures running. The only way to run a closure is when we are under an `Active` state. What if one closure wants to run under this state, and another decides to shut down the tenant? Do we want to continue executing?

In this specific RFC:

1. We use a `StateKeeper` / `StateSubscriber` / `StateHider` primitive to ensure all closures are gone, and running closures can't tell the difference.
2. It's proposed to run only non-cancellable short-living closures and check in the code reviews that the closure won't run for a long time. This should be easy to spot. Moreover: _all of the current code does not use long-running functions inside tenants_, even when GC or compaction is run.
3. To cancel GC and compaction, we could use the `CancelationSender` / `CancellationReceiver` pair. After they finish iteration, they check whether they should stop, and we wait for this to happen.

## `TimelineAccessor`

Works the same as `TenantAccessor`. Problems like [(#2442)](https://github.com/neondatabase/neon/issues/2442) are solved since we have to use `with_timeline` to lock on the timeline, and if GC or compaction is run, nothing will go wrong.

The owner of the timeline is the tenant, as it was before.

### States and transitions in `TimelineState`

We can use the same states for `TimelineState`. (Do we need any additional states for `TimelineState`?)

## `task_mgr` removal from tenant and timeline

With this RAII-like interface, we can easily remove `task_mgr` from tenant and timeline files.

Tasks runtime is basically the same as just running the closures, but this closure has some additional context about what we're executing, like:

1. Associated kind of task, `tenant_id` and `timeline_id`.
2. Name of the task.
3. Bool variable showing should we panic until the shutdown of the process or catch unwind?

The only really interesting property is 1. The join handles received from spawning the closure in runtime are being put in the global `HashMap` under a `Mutex`, and these tags allow us to select the tasks to shut down and to wait for completion. This leads to the global state linear time access under a mutex and hurts performance.

It appears we don't need this selector; we need a great design where join handles will be stored in the owner, more in a tree-like way. It appears our accessors can do this kind of thing!

1. `TenantAccessor` could be joined using the `DropWatcher`, so we can subscribe for a drop. The same applies to `TimelineGuard`.
2. The background tasks attached to the tenant or timeline are joined by the tenant or timeline itself when the drop ends.
3. For cancellation, we should only use the `CancelationSender` / `CancellationReceiver` in proper tasks. It behaves as `JoinGuard` with the possibility from another side to check whether we should stop.
4. After that, it's perfectly possible to run `spawn_blocking` tasks!
