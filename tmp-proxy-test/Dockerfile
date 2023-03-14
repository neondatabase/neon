FROM neondatabase/neon:latest

USER root
RUN apt-get update && apt-get install -y curl
RUN curl -fsSL https://deb.nodesource.com/setup_19.x | bash - && apt-get install -y nodejs

COPY . /tmp-proxy-test
RUN chown -R neon:neon /tmp-proxy-test
USER neon
RUN cd /tmp-proxy-test && npm install

ENTRYPOINT ["/bin/bash", "/tmp-proxy-test/entrypoint.sh"]
