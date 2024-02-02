## Release 202Y-MM-DD

**NB: this PR must be merged only by 'Create a merge commit'!**

### Checklist when preparing for release
- [ ] Read or refresh [the release flow guide](https://www.notion.so/neondatabase/Release-general-flow-61f2e39fd45d4d14a70c7749604bd70b)
- [ ] Ask in the [cloud Slack channel](https://neondb.slack.com/archives/C033A2WE6BZ) that you are going to rollout the release. Any blockers?
- [ ] Does this release contain any db migrations? Destructive ones? What is the rollback plan?

<!-- List everything that should be done **before** release, any issues / setting changes / etc -->

### Checklist after release
- [ ] Make sure instructions from PRs included in this release and labeled `manual_release_instructions` are executed (either by you or by people who wrote them).
- [ ] Based on the merged commits write release notes and open a PR into `website` repo ([example](https://github.com/neondatabase/website/pull/219/files))
- [ ] Check [#dev-production-stream](https://neondb.slack.com/archives/C03F5SM1N02) Slack channel
- [ ] Check [stuck projects page](https://console.neon.tech/admin/projects?sort=last_active&order=desc&stuck=true)
- [ ] Check [recent operation failures](https://console.neon.tech/admin/operations?action=create_timeline%2Cstart_compute%2Cstop_compute%2Csuspend_compute%2Capply_config%2Cdelete_timeline%2Cdelete_tenant%2Ccreate_branch%2Ccheck_availability&sort=updated_at&order=desc&had_retries=some)
- [ ] Check [cloud SLO dashboard](https://neonprod.grafana.net/d/_oWcBMJ7k/cloud-slos?orgId=1)
- [ ] Check [compute startup metrics dashboard](https://neonprod.grafana.net/d/5OkYJEmVz/compute-startup-time)

<!-- List everything that should be done **after** release, any admin UI configuration / Grafana dashboard / alert changes / setting changes / etc -->
