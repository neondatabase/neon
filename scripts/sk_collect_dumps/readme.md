# Collect /v1/debug_dump from all safekeeper nodes

3. Issue admin token (add/remove .stage from url for staging/prod and setting proper API key):
```
# staging:
AUTH_TOKEN=$(curl https://console-stage.neon.build/regions/console/api/v1/admin/issue_token -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer $NEON_STAGING_KEY" -X POST -d '{"ttl_seconds": 43200, "scope": "safekeeperdata"}' 2>/dev/null | jq --raw-output '.jwt')
# prod:
AUTH_TOKEN=$(curl https://console.neon.tech/regions/console/api/v1/admin/issue_token -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer $NEON_PROD_KEY" -X POST -d '{"ttl_seconds": 43200, "scope": "safekeeperdata"}' 2>/dev/null | jq --raw-output '.jwt')
# check
echo $AUTH_TOKEN
```
2. Run ansible playbooks to collect .json dumps from all safekeepers and store them in `./result` directory.

There are two ways to do that, with ssm or tsh. ssm:
```
# in aws repo, cd .github/ansible and run e.g. (adjusting profile and region in vars and limit):
AWS_DEFAULT_PROFILE=dev ansible-playbook -i inventory_aws_ec2.yaml -i staging.us-east-2.vars.yaml -e @ssm_config -l 'safekeeper:&us_east_2' -e "auth_token=${AUTH_TOKEN}" ~/neon/neon/scripts/sk_collect_dumps/remote.yaml
```
It will put the results to .results directory *near the playbook*.

tsh:

Update the inventory, if needed, selecting .build/.tech and optionally region:
```
rm -f hosts && echo '[safekeeper]' >> hosts
# staging:
tsh ls | awk '{print $1}' | grep safekeeper | grep "neon.build" | grep us-east-2 >> hosts
# prod:
tsh ls | awk '{print $1}' | grep safekeeper | grep "neon.tech" | grep us-east-2 >> hosts
```

Test ansible connection:
```
ansible all -m ping -v
```

Download the dumps:
```
mkdir -p result && rm -f result/*
ansible-playbook -e "auth_token=${AUTH_TOKEN}" remote.yaml
```

3. Run `DB_CONNSTR=... ./upload.sh prod_feb30` to upload dumps to `prod_feb30` table in specified postgres database.
