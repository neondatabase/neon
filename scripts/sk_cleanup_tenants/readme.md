# Cleanup script for safekeeper

This script can be used to remove tenant directories on safekeepers for projects which do not longer exist (deleted in console).

To run this script you need to upload it to safekeeper (i.e. with SSH), and run it with python3. Ansible can be used to run this script on multiple safekeepers.

NOTE: Console queries to check that project is deleted are slow and inefficient.
If you want to run this script on safekeeper with many tenants, consider
making PR to console repo to make projects search by tenant_id faster.

## How to run on a single node

```
zsh nsh safekeeper-0.us-east-2.aws.neon.build

ls /storage/safekeeper/data/ | grep -v safekeeper > tenants.txt

mkdir -p /storage/neon-trash/2023-01-01--cleanup

 export CONSOLE_API_TOKEN=
python3 script.py --trash-dir /storage/neon-trash/2023-01-01--cleanup --safekeeper-id $(cat /storage/safekeeper/data/safekeeper.id) --safekeeper-host $HOSTNAME --dry-run

cat tenants.txt | python3 script.py --trash-dir /storage/neon-trash/2023-01-01--cleanup --safekeeper-id $(cat /storage/safekeeper/data/safekeeper.id) --safekeeper-host $HOSTNAME --dry-run

cat tenants.txt | python3 script.py --trash-dir /storage/neon-trash/2023-01-01--cleanup --safekeeper-id $(cat /storage/safekeeper/data/safekeeper.id) --safekeeper-host $HOSTNAME |& tee logs.txt
```

## How to use ansible (staging)

```
cd ~/neon/.github/ansible

export AWS_DEFAULT_PROFILE=dev

ansible-playbook -i staging.us-east-2.hosts.yaml -e @ssm_config ../../scripts/sk_cleanup_tenants/remote.yaml

# add --extra-vars "api_token=" to set console api token
```

## How to use ansible (prod)

- Change `endpoint` in `script.py` to "https://console.neon.tech/api"

```
cd ~/neon/.github/ansible

export AWS_DEFAULT_PROFILE=prod

ansible-playbook -i prod.us-east-2.hosts.yaml -e @ssm_config ../../scripts/sk_cleanup_tenants/remote.yaml

# add --extra-vars "api_token=" to set console api token
```


> Heavily inspired with script for pageserver cleanup: https://gist.github.com/problame/bafb6ca6334f0145757238e61380c3f1/9bef1845a8291ebfa1f3a51eb79c01d12498b2b5