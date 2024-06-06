ARG REPOSITORY=neondatabase
ARG COMPUTE_IMAGE=compute-node-v14
ARG TAG=latest

FROM $REPOSITORY/${COMPUTE_IMAGE}:$TAG

USER root
RUN apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       python3-pip \
                       netcat
#Faker is required for the pg_anon test
RUN pip3 install Faker

USER postgres
# This is required for the pg_hintplan test, to prevent flaky log message causing test to fail
RUN touch /var/db/postgres/compute/compute_ctl_temp_override.conf