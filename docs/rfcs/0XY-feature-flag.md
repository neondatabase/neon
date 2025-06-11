# Storage Feature Flags

In this RFC, we will describe how we will implement per-tenant feature flags.

## PostHog as Feature Flag Service

Before we start, let's talk about how current feature flag services work. PostHog is the feature flag service we are currently using across multiple user-facing components in the company. PostHog has two modes of operation: HTTP evaluation and server-side local evaluation.

Let's assume we have a storage feature flag called gc-compaction and we want to roll it out to scale-tier users with resident size >= 10GB and <= 100GB.

### Define User Profiles

The first step is to synchronize our user profiles to the PostHog service. We can simply assume that each tenant is a user in PostHog. Each user profile has some properties associated with it. In our case, it will be: plan type (free, scale, enterprise, etc); resident size (in bytes); primary pageserver (string); region (string).

### Define Feature Flags

We would create a feature flag called gc-compaction in PostHog with 4 variants: disabled, stage-1, stage-2, fully-enabled. We will flip the feature flags from disabled to fully-enabled stage by stage for some percentage of our users.

### Option 1: HTTP Evaluation Mode

When using PostHog's HTTP evaluation mode, the client will make request to the PostHog service, asking for the value of a feature flag for a specific user.

* Control plane will report the plan type to PostHog each time it attaches a tenant to the storcon or when the user upgrades/downgrades. It calls the PostHog profile API to associate tenant ID with the plan type. Assume we have X active tenants and such attach or plan change event happens each week, that would be 4X profile update requests per month.
* Pageservers will report the resident size and the primary pageserver to the PostHog service. Assume we report resident size every 24 hours, that would be 30X requests per month.
* Each tenant will request the state of the feature flag every 1 hour, that's 720X requests per month.
* The Rust client would be easy to implement as we only need to call the `/decide` API on PostHog.

Using the HTTP evaluation mode we will issue 754X requests a month.

### Option 2: Local Evaluation Mode

When using PostHog's HTTP evaluation mode, the client (usually the server in a browser/server architecture) will poll the feature flag configuration every minute from PostHog. Such configuration contains data like:

<details>

<summary>Example JSON response from the PostHog local evaluation API</summary>

```
[
    {
        "id": 1,
        "name": "Beta Feature",
        "key": "person-flag",
        "is_simple_flag": True,
        "active": True,
        "filters": {
            "groups": [
                {
                    "properties": [
                        {
                            "key": "location",
                            "operator": "exact",
                            "value": ["Straße"],
                            "type": "person",
                        }
                    ],
                    "rollout_percentage": 100,
                },
                {
                    "properties": [
                        {
                            "key": "star",
                            "operator": "exact",
                            "value": ["ſun"],
                            "type": "person",
                        }
                    ],
                    "rollout_percentage": 100,
                },
            ],
        },
    }
]
```

</details>

Note that the API only contains information like "under what condition => rollout percentage". The user is responsible to provide the properties required to the client for local evaluation, and the PostHog service (web UI) cannot know if a feature is enabled for the tenant or not until the client uses the `capture` API to report the result back. To control the rollout percentage, the user ID gets mapped to a float number in `[0, 1)` on a consistent hash ring. All values <= the percentage will get the feature enabled or set to the desired value.

To use the local evaluation mode, the system needs:

* Each pageserver will poll PostHog for the local evaluation JSON every 5 minutes. That's 8640Y per month, Y is the number of pageservers. Local evaluation requests cost 10x more than the normal decide request, so that's 86400Y request units to bill.
* Storcon needs to store the plan type in the database and pass that information to the pageserver when attaching the tenant.
* Storcon also needs to update PostHog with the active tenants, for example, when the tenant gets detached/attached. Assume each active tenant gets detached/attached every week, that would be 4X requests per month.
* We do not need to update bill type or resident size to PostHog as all these are evaluated locally.
* After each local evaluation of the feature flag, we need to call PostHog's capture event API to update the result of the evaluation that the feature is enabled. We can do this when the flag gets changed compared with the last cached state in memory. That would be at least 4X (assume we do deployment every week so the cache gets cleared) and maybe an additional multiplifier of 10 assume we have 10 active features.

In this case, we will issue 86400Y + 40X requests per month.

Assume X = 1,000,000 and Y = 100,

|   | HTTP Evaluation  | Local Evaluation  |
|---|---|---|
| Latency of propagating the conditions/properties for feature flag  | 24 hours  | available locally  |
| Latency of applying the feature flag  | 1 hour  | 5 minutes  |
| Can properties be reported from different services |  Yes |  No  |
| Do we need to sync billing info etc to pageserver |  No |  Yes  |
| Cost | 75400$ / month | 4864$ / month |

# Our Solution

We will use PostHog _only_ as an UI to configure the feature flags. Whether a feature is enabled or not can only be queried through storcon. This allows us to ramp up the feature flag functionality fast at first. At the same time, it would also give us the option to migrate to our own solution once we want to have more properties and more complex evaluation rules in our system.

* We will create a single fake user in PostHog that contains all the properties we will use for evaluating a feature flag (i.e., resident size, billing type, pageserver id, etc.)
* We will use PostHog's local evaluation API to poll the configuration of the feature flags and evaluate them locally on each of the pageserver.
* The evaluation result will not be reported back to PostHog.
* Storcon needs to pull some information from cplane database.
* To know if a feature is currently enabled or not, we need to call the storcon/pageserver API; and we won't be able to know if a feature has been enabled on a tenant before easily: we need to look at the Grafana logs.

We only need to pay for the 86400Y local evaluation requests (that would be $864/month, and even less if we proxy it through storcon).

## Implementation

* Pageserver: implement a PostHog local evaluation client. The client will be shared across all tenants on the pageserver with a single API: `evaluate(tenant_id, feature_flag, properties) -> json`.
* Storcon: if we need plan type as the evaluation condition, pull it from cplane database.
* Storcon/Pageserver: implement an HTTP API `:tenant_id/feature/:feature` to retrieve the current feature flag status.

## Difference from Tenant Config

* Feature flags can be modified by percentage, and the default config for each feature flag can be modified in UI without going through the release process.
* Feature flags are more flexible and won't be persisted anywhere and will be passed as plain JSON over the wire so that do not need to handle backward/forward compatibility as in tenant config.
* The expectation of tenant config is that once we add a flag we cannot remove it (or it will be hard to remove), but feature flags are more flexible.

# Final Implementation

* We added a new crate `posthog_lite_client` that supports local feature evaluations.
* We set up two projects "Storage (staging)" and "Storage (production)" in the PostHog console.
* Each pageserver reports 10 fake tenants to PostHog so that we can get all combinations of regions (and other properties) in the PostHog UI.
* Supported properties: AZ, neon_region, pageserver, tenant_id.
* You may use "Pageserver Feature Flags" dashboard to see the evaluation status.

Each tenant has a `feature_resolver` object. After you add a feature flag in the PostHog console, you can retrieve it with:

```rust
// Boolean flag
self
    .feature_resolver
    .evaluate_boolean("flag", self.tenant_shard_id.tenant_id)
    .is_ok()
// Multivariate flag
self
    .feature_resolver
    .evaluate_multivariate("gc-comapction-strategy", self.tenant_shard_id.tenant_id)
    .ok();
```

The user needs to handle the case where the evaluation result is an error. This can occur in a variety of cases:

* During the pageserver start, the feature flag spec has not been retrieved.
* No condition group is matched.
* The feature flag spec contains an operand/operation not supported by the lite PostHog library.

For boolean flags, the return value is `Result<(), Error>`. `Ok(())` means the flag is evaluated to true. Otherwise,
there is either an error in evaluation or it does not match any groups.

For multivariate flags, the return value is `Result<String, Error>`. `Ok(variant)` indicates the flag is evaluated
to a variant. Otherwise, there is either an error in evaluation or it does not match any groups.

The evaluation logic is documented in the PostHog lite library. It compares the consistent hash of a flag key + tenant_id
with the rollout percentage and determines which tenant to roll out a specific feature.

Users can use the feature flag evaluation API to get the flag evaluation result of a specific tenant for debugging purposes.

```
curl http://localhost:9898/v1/tenant/:tenant_id/feature_flag?flag=:key&as=multivariate/boolean"
```

# Future Works

* Support dynamic tenant properties like logical size as the evaluation condition.
* Support properties like `plan_type` (needs cplane to pass it down)
* Report feature flag evaluation result back to PostHog (if the cost is okay)