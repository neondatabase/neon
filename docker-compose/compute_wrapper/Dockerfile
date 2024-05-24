ARG REPOSITORY=neondatabase
ARG COMPUTE_IMAGE=compute-node-v14
ARG TAG=latest

FROM $REPOSITORY/${COMPUTE_IMAGE}:$TAG

USER root
RUN apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       netcat

USER postgres
