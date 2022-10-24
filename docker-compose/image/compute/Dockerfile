ARG COMPUTE_IMAGE=compute-node-v14:latest
FROM neondatabase/${COMPUTE_IMAGE}

USER root
RUN apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       netcat

USER postgres
