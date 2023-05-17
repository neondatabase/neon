# Collect /v1/debug_dump from all safekeeper nodes

1. Run ansible playbooks to collect .json dumps from all safekeepers and store them in `./result` directory.
2. Run `DB_CONNSTR=... ./upload.sh prod_feb30` to upload dumps to `prod_feb30` table in specified postgres database.

## How to use ansible (staging)

```
AWS_DEFAULT_PROFILE=dev ansible-playbook -i ../../.github/ansible/staging.us-east-2.hosts.yaml -e @../../.github/ansible/ssm_config remote.yaml

AWS_DEFAULT_PROFILE=dev ansible-playbook -i ../../.github/ansible/staging.eu-west-1.hosts.yaml -e @../../.github/ansible/ssm_config remote.yaml
```

## How to use ansible (prod)

```
AWS_DEFAULT_PROFILE=prod ansible-playbook -i ../../.github/ansible/prod.us-west-2.hosts.yaml -e @../../.github/ansible/ssm_config remote.yaml

AWS_DEFAULT_PROFILE=prod ansible-playbook -i ../../.github/ansible/prod.us-east-2.hosts.yaml -e @../../.github/ansible/ssm_config remote.yaml

AWS_DEFAULT_PROFILE=prod ansible-playbook -i ../../.github/ansible/prod.eu-central-1.hosts.yaml -e @../../.github/ansible/ssm_config remote.yaml

AWS_DEFAULT_PROFILE=prod ansible-playbook -i ../../.github/ansible/prod.ap-southeast-1.hosts.yaml -e @../../.github/ansible/ssm_config remote.yaml
```

