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
RUN pip3 install Faker
RUN mkdir -p /ext-src/pg_hint_plan-src/data && chown postgres /ext-src/pg_hint_plan-src/data

USER postgres
