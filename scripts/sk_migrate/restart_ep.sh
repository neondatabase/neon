#!/bin/bash

# export NEON_API_KEY=

while IFS= read -r ENDPOINT
do
    echo "$ENDPOINT"
    # curl -X POST -H "Authorization: Bearer $NEON_PROD_KEY" -H "Accept: application/json" -H "Content-Type: application/json"  https://console.neon.tech/regions/console/api/v1/admin/endpoints/$ENDPOINT/restart
    curl -X POST -H "Authorization: Bearer $NEON_API_KEY" -H "Accept: application/json" -H "Content-Type: application/json"  https://console.neon.tech/regions/aws-us-east-2/api/v1/admin/endpoints/$ENDPOINT/restart
done < endpoints_cplane.txt