#!/bin/bash
#
# NOTE: You should set the BASIC_AUTH_PASSWORD environment variable before calling

# Launch S3 server
(cd ms3 && python3 -m ms3.app --listen-address=localhost) &

FLASK_ENV=development S3_REGION=auto S3_ENDPOINT=http://localhost:9009 S3_BUCKET=zenith-testbucket PATH=/home/heikki/pgsql.fsmfork/bin:$PATH flask run --host=0.0.0.0
