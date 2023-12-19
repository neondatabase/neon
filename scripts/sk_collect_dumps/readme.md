# Collect /v1/debug_dump from all safekeeper nodes

3. Issue admin token (add/remove .stage from url for staging/prod and setting proper API key):
```
AUTH_TOKEN=$(curl https://console.stage.neon.tech/regions/console/api/v1/admin/issue_token -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer $NEON_STAGING_KEY" -X POST -d '{"ttl_seconds": 43200, "scope": "safekeeperdata"}' 2>/dev/null | jq --raw-output '.jwt')
# check
echo $AUTH_TOKEN
```
2. Run ansible playbooks to collect .json dumps from all safekeepers and store them in `./result` directory.
```
# in aws repo, cd .github/ansible and run e.g. (ajusting profile and region in vars and limit):
AWS_DEFAULT_PROFILE=dev ansible-playbook -i inventory_aws_ec2.yaml -i staging.us-east-2.vars.yaml -e @ssm_config -l 'safekeeper:&us_east_2' -e "auth_token=${AUTH_TOKEN}" --check ~/neon/neon/scripts/sk_collect_dumps/remote.yaml
```
It will put the results to .results directory *near the playbook*.
2. Run `DB_CONNSTR=... ./upload.sh prod_feb30` to upload dumps to `prod_feb30` table in specified postgres database.
