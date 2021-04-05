#!/bin/bash
#
# NOTE: You must set the following environment variables before running this:
#  BASIC_AUTH_PASSWORD - basic http auth password
#  S3_ACCESSKEY
#  S3_SECRET


S3_ENDPOINT=https://storage.googleapis.com S3_BUCKET=zenith-testbucket PATH=/home/heikki/pgsql-install/bin:$PATH flask run --host=0.0.0.0
