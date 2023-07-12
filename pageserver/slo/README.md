
## Install Sloth

https://sloth.dev/introduction/install/

```sh
wget https://github.com/slok/sloth/releases/download/v0.11.0/sloth-linux-amd64
chmod +x ./sloth-linux-amd64
```

## Background on Sloth

https://sloth.dev/introduction/
https://sloth.dev/introduction/architecture/


## Generate Prometheus Rules From Sloth Spec

```
./sloth-linux-amd64 generate --input ./spec.sloth.yml --out generated.prometheus.rules.yml
```

## Background reading:

SRE workbook chapter on "Implementing SLOs", section "Calculating the SLIs"

https://sre.google/workbook/implementing-slos/

Citation:

```
Availability

    sum(rate(http_requests_total{host="api", status!~"5.."}[7d]))
    /
    sum(rate(http_requests_total{host="api"}[7d])

Latency

    histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[7d]))

    histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[7d]))
```

## Sloth Rule Syntax

It's under-documented.

Best to go to the Go types:
https://pkg.go.dev/github.com/slok/sloth@v0.6.0/pkg/prometheus/api/v1#section-readme

For latency SLOs,  pageserver, we want the "Raw SLI" type SLI, not the one that is based on events.
Seach for `error_ratio_query` ; example: https://sloth.dev/examples/default/raw-sli/

Use victoriametrics `histogram_share` to compute the error ratio.
It's the inverese of histogram_quantile.
https://docs.victoriametrics.com/MetricsQL.html#histogram_share

`share_le_over_time` seems also useful
https://docs.victoriametrics.com/MetricsQL.html#share_le_over_time

https://stackoverflow.com/questions/72559302/is-it-possible-to-calculate-ranks-of-metrics?rq=1

Problem with the VictoriaMetrics-only functions is that sloth has an internal validation pass:
https://github.com/slok/sloth/issues/510
Option to skip the check:
https://github.com/slok/sloth/pull/511
=>
```
git submodule update --init
pushd sloth.git
make build
popd
sloth.git/bin/sloth-linux-amd64 generate  \
    --disable-promExpr-validation \
    --input ./spec.sloth.yml \
    --out generated.prometheus.rules.yml
```

## Notes On How To Scale The Process To Multiple Teams / Automate Sloth In Neon

* SLO directory discovery: https://sloth.dev/usage/cli/
  * allows using directories instead of indivudal files as input
