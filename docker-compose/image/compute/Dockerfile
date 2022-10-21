FROM neondatabase/compute-node:2221

USER root
RUN apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       netcat

USER postgres