#!/bin/bash

set -o xtrace  # Print each command before execution

PGPASSWORD=password psql -h localhost -U postgres -p 8432 -d dockercplane -c "select name, postgres_version from branches where deleted=false;"