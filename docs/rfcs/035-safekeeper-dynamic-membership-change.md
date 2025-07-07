# Safekeeper dynamic membership change

To quickly recover from safekeeper node failures and do rebalancing we need to
be able to change set of safekeepers the timeline resides on. The procedure must
be safe (not lose committed log) regardless of safekeepers and compute state. It
should be able to progress if any majority of old safekeeper set, any majority
of new safekeeper set and compute are up and connected. This is known as a
consensus membership change. It always involves two phases: 1) switch old
majority to old + new configuration, preventing commits without acknowledge from
the new set 2) bootstrap the new set by ensuring majority of the new set has all
data which ever could have been committed before the first phase completed;
after that switch is safe to finish. Without two phases switch to the new set
which quorum might not intersect with quorum of the old set (and typical case of
ABC -> ABD switch is an example of that, because quorums AC and BD don't
intersect). Furthermore, procedure is typically carried out by the consensus
leader, and so enumeration of configurations which establishes order between
them is done through consensus log.

In our case consensus leader is compute (walproposer), and we don't want to wake
up all computes for the change. Neither we want to fully reimplement the leader
logic second time outside compute. Because of that the proposed algorithm relies
for issuing configurations on the external fault tolerant (distributed) strongly
consisent storage with simple API: CAS (compare-and-swap) on the single key.
Properly configured postgres suits this.

In the system consensus is implemented at the timeline level, so algorithm below
applies to the single timeline.

## Algorithm

### Definitions

A configuration is

```
struct Configuration {
    generation: Generation, // a number uniquely identifying configuration
    sk_set: Vec<NodeId>, // current safekeeper set
    new_sk_set: Optional<Vec<NodeId>>,
}
```

Configuration with `new_set` present is used for the intermediate step during
the change and called joint configuration. Generations establish order of
generations: we say `c1` is higher than `c2` if `c1.generation` >
`c2.generation`.

### Persistently stored data changes

Safekeeper starts storing its current configuration in the control file. Update
of is atomic, so in-memory value always matches the persistent one.

External CAS providing storage (let's call it configuration storage here) also
stores configuration for each timeline. It is initialized with generation 1 and
initial set of safekeepers during timeline creation. Executed CAS on it must
never be lost.

### Compute <-> safekeeper protocol changes

`ProposerGreeting` message carries walproposer's configuration if it is already
established (see below), else null.  `AcceptorGreeting` message carries
safekeeper's current `Configuration`. All further messages (`VoteRequest`,
`VoteResponse`, `ProposerElected`, `AppendRequest`, `AppendResponse`) carry
generation number, of walproposer in case of wp->sk message or of safekeeper in
case of sk->wp message.

### Safekeeper changes

Basic rule: once safekeeper observes configuration higher than his own it
immediately switches to it. It must refuse all messages with lower generation
that his. It also refuses messages if it is not member of the current generation
(that is, of either `sk_set` of `sk_new_set`), though it is likely not unsafe to
process them (walproposer should ignore result anyway).

If there is non null configuration in `ProposerGreeting` and it is higher than
current safekeeper one, safekeeper switches to it.

Safekeeper sends its current configuration in its first message to walproposer
`AcceptorGreeting`. It refuses all other walproposer messages if the
configuration generation in them is less than its current one. Namely, it
refuses to vote, to truncate WAL in `handle_elected` and to accept WAL. In
response it sends its current configuration generation to let walproposer know.

Safekeeper gets `PUT /v1/tenants/{tenant_id}/timelines/{timeline_id}/configuration`
accepting `Configuration`. Safekeeper switches to the given conf it is higher than its
current one and ignores it otherwise. In any case it replies with
```
struct ConfigurationSwitchResponse {
    conf: Configuration,
    term: Term,
    last_log_term: Term,
    flush_lsn: Lsn,
}
```

### Compute (walproposer) changes

Basic rule is that joint configuration requires votes from majorities in the
both `set` and `new_sk_set`.

Compute receives list of safekeepers to connect to from the control plane as
currently and tries to communicate with all of them. However, the list does not
define consensus members. Instead, on start walproposer tracks highest
configuration it receives from `AcceptorGreeting`s. Once it assembles greetings
from majority of `sk_set` and majority of `new_sk_set` (if it is present), it
establishes this configuration as its own and moves to voting.

It should stop talking to safekeepers not listed in the configuration at this
point, though it is not unsafe to continue doing so.

To be elected it must receive votes from both majorites if `new_sk_set` is present.
Similarly, to commit WAL it must receive flush acknowledge from both majorities.

If walproposer hears from safekeeper configuration higher than his own (i.e.
refusal to accept due to configuration change) it simply restarts.

### Change algorithm

The following algorithm can be executed anywhere having access to configuration
storage and safekeepers. It is safe to interrupt / restart it and run multiple
instances of it concurrently, though likely one of them won't make
progress then. It accepts `desired_set: Vec<NodeId>` as input.

Algorithm will refuse to make the change if it encounters previous interrupted
change attempt, but in this case it will try to finish it.

It will eventually converge if old majority, new majority and configuration
storage are reachable.

1) Fetch current timeline configuration from the configuration storage.
2) If it is already joint one and `new_set` is different from `desired_set`
   refuse to change. However, assign join conf to (in memory) var
   `join_conf` and proceed to step 4 to finish the ongoing change.
3) Else, create joint `joint_conf: Configuration`: increment current conf number
   `n` and put `desired_set` to `new_sk_set`. Persist it in the configuration
   storage by doing CAS on the current generation: change happens only if
   current configuration number is still `n`. Apart from guaranteeing uniqueness
   of configurations, CAS linearizes them, ensuring that new configuration is
   created only following the previous one when we know that the transition is
   safe. Failed CAS aborts the procedure.
4) Call `PUT` `configuration` on safekeepers from the current set,
   delivering them `joint_conf`. Collecting responses from majority is required
   to proceed. If any response returned generation higher than
   `joint_conf.generation`, abort (another switch raced us). Otherwise, choose
   max `<last_log_term, flush_lsn>` among responses and establish it as
   (in memory) `sync_position`. Also choose max `term` and establish it as (in
   memory) `sync_term`. We can't finish the switch until majority of the new set
   catches up to this `sync_position` because data before it could be committed
   without ack from the new set. Similarly, we'll bump term on new majority
   to `sync_term` so that two computes with the same term are never elected.
4) Initialize timeline on safekeeper(s) from `new_sk_set` where it
   doesn't exist yet by doing `pull_timeline` from the majority of the
   current set. Doing that on majority of `new_sk_set` is enough to
   proceed, but it is reasonable to ensure that all `new_sk_set` members
   are initialized -- if some of them are down why are we migrating there?
5) Call `POST` `bump_term(sync_term)` on safekeepers from the new set.
   Success on majority is enough.
6) Repeatedly call `PUT` `configuration` on safekeepers from the new set,
   delivering them `joint_conf` and collecting their positions. This will
   switch them to the `joint_conf` which generally won't be needed
   because `pull_timeline` already includes it and plus additionally would be
   broadcast by compute. More importantly, we may proceed to the next step
   only when `<last_log_term, flush_lsn>` on the majority of the new set reached
   `sync_position`. Similarly, on the happy path no waiting is not needed because
   `pull_timeline` already includes it. However, we should double
    check to be safe. For example, timeline could have been created earlier e.g.
    manually or after try-to-migrate, abort, try-to-migrate-again sequence.
7) Create `new_conf: Configuration` incrementing `join_conf` generation and having new
   safekeeper set as `sk_set` and None `new_sk_set`. Write it to configuration
   storage under one more CAS.
8) Call `PUT` `configuration` on safekeepers from the new set,
   delivering them `new_conf`. It is enough to deliver it to the majority
   of the new set; the rest can be updated by compute.

I haven't put huge effort to make the description above very precise, because it
is natural language prone to interpretations anyway. Instead I'd like to make TLA+
spec of it.

Description above focuses on safety. To make the flow practical and live, here a few more
considerations.
1) It makes sense to ping new set to ensure it we are migrating to live node(s) before
  step 3.
2) If e.g. accidentally wrong new sk set has been specified, before CAS in step `6` is completed
   it is safe to rollback to the old conf with one more CAS.
3) On step 4 timeline might be already created on members of the new set for various reasons;
   the simplest is the procedure restart. There are more complicated scenarious like mentioned
   in step 5. Deleting and re-doing `pull_timeline` is generally unsafe without involving
   generations, so seems simpler to treat existing timeline as success. However, this also
   has a disadvantage: you might imagine an surpassingly unlikely schedule where condition in
   the step 5 is never reached until compute is (re)awaken up to synchronize new member(s).
   I don't think we'll observe this in practice, but can add waking up compute if needed.
4) In the end timeline should be locally deleted on the safekeeper(s) which are
   in the old set but not in the new one, unless they are unreachable. To be
   safe this also should be done under generation number (deletion proceeds only if
   current configuration is <= than one in request and safekeeper is not memeber of it).
5) If current conf fetched on step 1 is already not joint and members equal to `desired_set`,
   jump to step 7, using it as `new_conf`.

## Implementation

The procedure ought to be driven from somewhere. Obvious candidates are control
plane and storage_controller; and as each of them already has db we don't want
yet another storage. I propose to manage safekeepers in storage_controller
because 1) since it is in rust it simplifies simulation testing (more on this
below) 2) it already manages pageservers.

This assumes that migration will be fully usable only after we migrate all
tenants/timelines to storage_controller. It is discussible whether we want also
to manage pageserver attachments for all of these, but likely we do.

This requires us to define storcon <-> cplane interface and changes.

### storage_controller <-> control plane interface and changes

First of all, control plane should
[change](https://neondb.slack.com/archives/C03438W3FLZ/p1719226543199829)
storing safekeepers per timeline instead of per tenant because we can't migrate
tenants atomically.

The important question is how updated configuration is delivered from
storage_controller to control plane to provide it to computes. As always, there
are two options, pull and push. Let's do it the same push as with pageserver
`/notify-attach` because 1) it keeps storage_controller out of critical compute
start path 2) uniformity. It makes storage_controller responsible for retrying
notifying control plane until it succeeds.

It is not needed for the control plane to fully know the `Configuration`. It is
enough for it to only to be aware of the list of safekeepers in the latest
configuration to supply it to compute, plus associated generation number to
protect from stale update requests and to also pass it to compute.

So, cplane `/notify-safekeepers` for the timeline can accept JSON like
```
{
   tenant_id: String,
   timeline_id: String,
   generation: u32,
   safekeepers: Vec<SafekeeperId>,
}
```
where `SafekeeperId` is
```
{
   node_id: u64,
   host: String
}
```
In principle `host` is redundant, but may be useful for observability.

The request updates list of safekeepers in the db if the provided conf
generation is higher (the cplane db should also store generations for this).
Similarly to
[`/notify-attach`](https://www.notion.so/neondatabase/Storage-Controller-Control-Plane-interface-6de56dd310a043bfa5c2f5564fa98365),
it should update db which makes the call successful, and then try to schedule
`apply_config` if possible, it is ok if not. storage_controller should rate
limit calling the endpoint, but likely this won't be needed, as migration
throughput is limited by `pull_timeline`.

Timeline (branch) creation in cplane should call storage_controller POST
`tenant/:tenant_id/timeline` like it currently does for sharded tenants.
Response should be augmented with `safekeepers_generation` and `safekeepers`
fields like described in `/notify-safekeepers` above. Initially (currently)
these fields may be absent; in this case cplane chooses safekeepers on its own
like it currently does. The call should be retried until succeeds.

Timeline deletion and tenant deletion in cplane should call appropriate
storage_controller endpoints like it currently does for sharded tenants. The
calls should be retried until they succeed.

When compute receives safekeepers list from control plane it needs to know the
generation to checked whether it should be updated (note that compute may get
safekeeper list from either cplane or safekeepers). Currently `neon.safekeepers`
GUC is just a comma separates list of `host:port`. Let's prefix it with
`g#<generation>:` to this end, so it will look like
```
g#42:safekeeper-0.eu-central-1.aws.neon.tech:6401,safekeeper-2.eu-central-1.aws.neon.tech:6401,safekeeper-1.eu-central-1.aws.neon.tech:6401
```

To summarize, list of cplane changes:
- per tenant -> per timeline safekeepers management and addition of int `safekeeper_generation` field.
- `/notify-safekeepers` endpoint.
- Branch creation call may return list of safekeepers and when it is
  present cplane should adopt it instead of choosing on its own like it does currently.
- `neon.safekeepers` GUC should be prefixed with `g#<generation>:`.

### storage_controller implementation

If desired, we may continue using current 'load everything on startup and keep
in memory' approach: single timeline shouldn't take more than 100 bytes (it's 16
byte tenant_id, 16 byte timeline_id, int generation, vec of ~3 safekeeper ids
plus some flags), so 10^6 of timelines shouldn't take more than 100MB.

Similar to pageserver attachment Intents storage_controller would have in-memory
`MigrationRequest` (or its absense) for each timeline and pool of tasks trying
to make these request reality; this ensures one instance of storage_controller
won't do several migrations on the same timeline concurrently. In the first
version it is simpler to have more manual control and no retries, i.e. migration
failure removes the request. Later we can build retries and automatic
scheduling/migration around. `MigrationRequest` is
```
enum MigrationRequest {
    To(Vec<NodeId>),
    FinishPending,
}
```

`FinishPending` requests to run the procedure to ensure state is clean: current
configuration is not joint and majority of safekeepers are aware of it, but do
not attempt to migrate anywhere. If current configuration fetched on step 1 is
not joint it jumps to step 7. It should be run at startup for all timelines (but
similarly, in the first version it is ok to trigger it manually).

#### Schema

`safekeepers` table mirroring current `nodes` should be added, except that for
`scheduling_policy`: it is enough to have at least in the beginning only 3
fields: 1) `active` 2) `paused` (initially means only not assign new tlis there
3) `decomissioned` (node is removed).

`timelines` table:
```
table! {
    // timeline_id is primary key
    timelines (tenant_id, timeline_id) {
        timeline_id -> Varchar,
        tenant_id -> Varchar,
        start_lsn -> pg_lsn,
        generation -> Int4,
        sk_set -> Array<Int4>, // list of safekeeper ids
        new_sk_set -> Nullable<Array<Int8>>, // list of safekeeper ids, null if not joint conf
        cplane_notified_generation -> Int4,
        deleted_at -> Nullable<Timestamptz>,
    }
}
```

`start_lsn` is needed to create timeline on safekeepers properly, see below. We
might also want to add ancestor_timeline_id to preserve the hierarchy, but for
this RFC it is not needed.

#### API

Node management is similar to pageserver:
1) POST `/control/v1/safekeepers` inserts safekeeper.
2) GET `/control/v1/safekeepers` lists safekeepers.
3) GET `/control/v1/safekeepers/:node_id` gets safekeeper.
4) PUT `/control/v1/safekepers/:node_id/status` changes status to e.g.
   `offline` or `decomissioned`. Initially it is simpler not to schedule any
    migrations here.

Safekeeper deploy scripts should register safekeeper at storage_contorller as
they currently do with cplane, under the same id.

Timeline creation/deletion will work through already existing POST and DELETE
`tenant/:tenant_id/timeline`. Cplane is expected to retry both until they
succeed. See next section on the implementation details.

We don't want to block timeline creation/deletion when one safekeeper is down.
Currently this is crutched by compute implicitly creating timeline on any
safekeeper it is connected to. This creates ugly timeline state on safekeeper
when timeline is created, but start LSN is not defined yet. Next section
describes dealing with this.

Tenant deletion repeats timeline deletion for all timelines.

Migration API: the first version is the simplest and the most imperative:
1) PUT `/control/v1/safekeepers/migrate` schedules `MigrationRequest`s to move
all timelines from one safekeeper to another. It accepts json
```
{
    "src_sk": u32,
    "dst_sk": u32,
    "limit": Optional<u32>,
}
```

Returns list of scheduled requests.

2) PUT `/control/v1/tenant/:tenant_id/timeline/:timeline_id/safekeeper_migrate` schedules `MigrationRequest`
   to move single timeline to given set of safekeepers:
```
{
    "desired_set": Vec<u32>,
}
```

Returns scheduled request.

Similar call should be added for the tenant.

It would be great to have some way of subscribing to the results (apart from
looking at logs/metrics).

3) GET `/control/v1/tenant/:tenant_id/timeline/:timeline_id/` should return
   current in memory state of the timeline and pending `MigrationRequest`,
   if any.

4) PUT `/control/v1/tenant/:tenant_id/timeline/:timeline_id/safekeeper_migrate_abort` tries to abort the
   migration by switching configuration from the joint to the one with (previous) `sk_set` under CAS
   (incrementing generation as always).

#### API implementation and reconciliation

For timeline creation/deletion we want to preserve the basic assumption that
unreachable minority (1 sk of 3) doesn't block their completion, but eventually
we want to finish creation/deletion on nodes which missed it (unless they are
removed). Similarly for migration; it may and should finish even though excluded
members missed their exclusion. And of course e.g. such pending exclusion on
node C after migration ABC -> ABD must not prevent next migration ABD -> ABE. As
another example, if some node missed timeline creation it clearly must not block
migration from it. Hence it is natural to have per safekeeper background
reconciler which retries these ops until they succeed. There are 3 possible
operation types, and the type is defined by timeline state (membership
configuration and whether it is deleted) and safekeeper id: we may need to
create timeline on sk (node added), locally delete it (node excluded, somewhat
similar to detach) or globally delete it (timeline is deleted).

Next, on storage controller restart in principle these pending operations can be
figured out by comparing safekeepers state against storcon state. But it seems
better to me to materialize them in the database; it is not expensive, avoids
these startup scans which themselves can fail etc and makes it very easy to see
outstanding work directly at the source of truth -- the db. So we can add table
`safekeeper_timeline_pending_ops`
```
table! {
    // timeline_id, sk_id is primary key
    safekeeper_timeline_pending_ops (sk_id, tenant_id, timeline_id) {
        sk_id -> int8,
        tenant_id -> Varchar,
        timeline_id -> Varchar,
        generation -> Int4,
        op_type -> Varchar,
    }
}
```

`op_type` can be `include` (seed from peers and ensure generation is up to
date), `exclude` (remove locally) and `delete`. Field is actually not strictly
needed as it can be computed from current configuration, but gives more explicit
observability.

`generation` is necessary there because after op is done reconciler must remove
it and not remove another row with higher gen which in theory might appear.

Any insert of row should overwrite (remove) all rows with the same sk and
timeline id but lower `generation` as next op makes previous obsolete. Insertion
of `op_type` `delete` overwrites all rows.

About `exclude`: rather than adding explicit safekeeper http endpoint, it is
reasonable to reuse membership switch endpoint: if safekeeper is not member
of the configuration it locally removes the timeline on the switch. In this case
404 should also be considered an 'ok' answer by the caller.

So, main loop of per sk reconcile reads `safekeeper_timeline_pending_ops`
joined with timeline configuration to get current conf (with generation `n`)
for the safekeeper and does the jobs, infinitely retrying failures:
1) If node is member (`include`):
  - Check if timeline exists on it, if not, call pull_timeline on it from 
     other members
  - Call switch configuration to the current
2) If node is not member (`exclude`):
  - Call switch configuration to the current, 404 is ok.
3) If timeline is deleted (`delete`), call delete.

In cases 1 and 2 remove `safekeeper_timeline_pending_ops` for the sk and 
timeline with generation <= `n` if `op_type` is not `delete`.
In case 3 also remove `safekeeper_timeline_pending_ops` 
entry + remove `timelines` entry if there is nothing left  in `safekeeper_timeline_pending_ops` for the timeline.

Let's consider in details how APIs can be implemented from this angle.

Timeline creation. It is assumed that cplane retries it until success, so all
actions must be idempotent. Now, a tricky point here is timeline start LSN. For
the initial (tenant creation) call cplane doesn't know it. However, setting
start_lsn on safekeepers during creation is a good thing -- it provides a
guarantee that walproposer can always find a common point in WAL histories of
safekeeper and its own, and so absense of it would be a clear sign of
corruption. The following sequence works:
1) Create timeline (or observe that it exists) on pageserver,
   figuring out last_record_lsn in response.
2) Choose safekeepers and insert (ON CONFLICT DO NOTHING) timeline row into the
   db. Note that last_record_lsn returned on the previous step is movable as it
   changes once ingestion starts, insert must not overwrite it (as well as other
   fields like membership conf). On the contrary, start_lsn used in the next
   step must be set to the value in the db. cplane_notified_generation can be set
   to 1 (initial generation) in insert to avoid notifying cplane about initial 
   conf as cplane will receive it in timeline creation request anyway.
3) Issue timeline creation calls to at least majority of safekeepers. Using
   majority here is not necessary but handy because it guarantees that any live
   majority will have at least one sk with created timeline and so
   reconciliation task can use pull_timeline shared with migration instead of
   create timeline special init case. OFC if timeline is already exists call is
   ignored.
4) For minority of safekeepers which could have missed creation insert
   entries to `safekeeper_timeline_pending_ops`. We won't miss this insertion 
   because response to cplane is sent only after it has happened, and cplane 
   retries the call until 200 response.

   There is a small question how request handler (timeline creation in this
   case) would interact with per sk reconciler. As always I prefer to do the
   simplest possible thing and here it seems to be just waking it up so it
   re-reads the db for work to do. Passing work in memory is faster, but
   that shouldn't matter, and path to scan db for work will exist anyway, 
   simpler to reuse it.

For pg version / wal segment size: while we may persist them in `timelines`
table, it is not necessary as initial creation at step 3 can take them from
pageserver or cplane creation call and later pull_timeline will carry them
around.

Timeline migration.
1) CAS to the db to create joint conf, and in the same transaction create
   `safekeeper_timeline_pending_ops` `include` entries to initialize new members
   as well as deliver this conf to current ones; poke per sk reconcilers to work
   on it. Also any conf change should also poke cplane notifier task(s).
2) Once it becomes possible per alg description above, get out of joint conf
   with another CAS. Task should get wakeups from per sk reconcilers because 
   conf switch is required for advancement; however retries should be sleep
   based as well as LSN advancement might be needed, though in happy path 
   it isn't. To see whether further transition is possible on wakup migration
   executor polls safekeepers per the algorithm. CAS creating new conf with only
   new members should again insert entries to `safekeeper_timeline_pending_ops`
   to switch them there, as well as `exclude` rows to remove timeline from 
   old members.

Timeline deletion: just set `deleted_at` on the timeline row and insert
`safekeeper_timeline_pending_ops` entries in the same xact, the rest is done by
per sk reconcilers.

When node is removed (set to `decomissioned`), `safekeeper_timeline_pending_ops`
for it must be cleared in the same transaction.

One more task pool should infinitely retry notifying control plane about changed
safekeeper sets (trying making `cplane_notified_generation` equal `generation`).

#### Dealing with multiple instances of storage_controller

Operations described above executed concurrently might create some errors but do
not prevent progress, so while we normally don't want to run multiple instances
of storage_controller it is fine to have it temporarily, e.g. during redeploy.

To harden against some controller instance creating some work in
`safekeeper_timeline_pending_ops` and then disappearing without anyone pickup up
the job per sk reconcilers apart from explicit wakups should scan for work
periodically. It is possible to remove that though if all db updates are
protected with leadership token/term -- then such scans are needed only after
leadership is acquired.

Any interactions with db update in-memory controller state, e.g. if migration
request failed because different one is in progress, controller remembers that
and tries to finish it.

## Testing

`neon_local` should be switched to use storage_controller, playing role of
control plane.

There should be following layers of tests:
1) Model checked TLA+ spec specifies the algorithm and verifies its basic safety.

2) To cover real code and at the same time test many schedules we should have
   simulation tests. For that, configuration storage, storage_controller <->
   safekeeper communication and pull_timeline need to be mocked and main switch
   procedure wrapped to as a node (thread) in simulation tests, using these
   mocks. Test would inject migrations like it currently injects
   safekeeper/walproposer restars. Main assert is the same -- committed WAL must
   not be lost.

3) Since simulation testing injects at relatively high level points (not
   syscalls), it omits some code, in particular `pull_timeline`. Thus it is
   better to have basic tests covering whole system as well. Extended version of
   `test_restarts_under_load` would do: start background load and do migration
   under it, then restart endpoint and check that no reported commits
   had been lost. I'd also add one more creating classic network split scenario, with
   one compute talking to AC and another to BD while migration from nodes ABC to ABD
   happens.

4) Simple e2e test should ensure that full flow including cplane notification works.

## Order of implementation and rollout

Note that
- Control plane parts and integration with it is fully independent from everything else
  (tests would use simulation and neon_local).
- It is reasonable to make compute <-> safekeepers protocol change
  independent of enabling generations.
- There is a lot of infra work making storage_controller aware of timelines and safekeepers
  and its impl/rollout should be separate from migration itself.
- Initially walproposer can just stop working while it observes joint configuration.
  Such window would be typically very short anyway.
- Obviously we want to test the whole thing thoroughly on staging and only then
  gradually enable in prod.

Let's have the following implementation bits for gradual rollout:
- compute gets `neon.safekeepers_proto_version` flag.
  Initially both compute and safekeepers will be able to talk both
  versions so that we can delay force restart of them and for
  simplicity of rollback in case it is needed.
- storcon gets `-set-safekeepers` config option disabled by
  default. Timeline creation request chooses safekeepers
  (and returns them in response to cplane) only when it is set to
  true.
- control_plane [see above](storage_controller-<->-control-plane interface-and-changes)
  prefixes `neon.safekeepers` GUC with generation number. When it is 0
  (or prefix not present at all), walproposer behaves as currently, committing on
  the provided safekeeper list -- generations are disabled.
  If it is non 0 it follows this RFC rules.
- We provide a script for manual migration to storage controller.
  It selects timeline(s) from control plane (specified or all of them) db
  and calls special import endpoint on storage controller which is very
  similar to timeline creation: it inserts into the db, sets
  configuration to initial on the safekeepers, calls cplane
  `notify-safekeepers`.

Then the rollout for a region would be:
- Current situation: safekeepers are choosen by control_plane.
- We manually migrate some timelines, test moving them around.
- Then we enable `--set-safekeepers` so that all new timelines
  are on storage controller.
- Finally migrate all existing timelines using the script (no
  compute should be speaking old proto version at this point).

Until all timelines are managed by storcon we'd need to use current ad hoc
script to migrate if needed. To keep state clean, all storage controller managed
timelines must be migrated before that, or controller db and configurations
state of safekeepers dropped manually.

Very rough implementation order:
- Add concept of configurations to safekeepers (including control file),
  implement v3 protocol.
- Implement walproposer changes, including protocol.
- Implement storconn part. Use it in neon_local (and pytest).
- Make cplane store safekeepers per timeline instead of per tenant.
- Implement cplane/storcon integration. Route branch creation/deletion
  through storcon. Then we can test migration of new branches.
- Finally import existing branches. Then we can drop cplane
  safekeeper selection code. Gradually enable configurations at
  computes and safekeepers. Before that, all computes must talk only
  v3 protocol version.

## Integration with evicted timelines

Currently, `pull_timeline` doesn't work correctly with evicted timelines because
copy would point to original partial file. To fix let's just do s3 copy of the
file. It is a bit stupid as generally unnecessary work, but it makes sense to
implement proper migration before doing smarter timeline archival. [Issue](https://github.com/neondatabase/neon/issues/8542)

## Possible optimizations

Steps above suggest walproposer restart (with re-election) and thus reconnection
to safekeepers. Since by bumping term on new majority we ensure that leader
terms are unique even across generation switches it is possible to preserve
connections. However, it is more complicated, reconnection is very fast and it
is much more important to avoid compute restart than millisecond order of write
stall.

Multiple joint consensus: algorithm above rejects attempt to change membership
while another attempt is in progress. It is possible to overlay them and AFAIK
Aurora does this but similarly I don't think this is needed.

## Misc

We should use Compute <-> safekeeper protocol change to include other (long
yearned) modifications:
- send data in network order without putting whole structs to be arch independent
- remove term_start_lsn from AppendRequest
- add horizon to TermHistory
- add to ProposerGreeting number of connection from this wp to sk
