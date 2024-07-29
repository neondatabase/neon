FROM debian:bullseye-slim

# Use ARG as a build-time environment variable here to allow.
# It's not supposed to be set outside.
# Alternatively it can be obtained using the following command
# ```
# . /etc/os-release && echo "${VERSION_CODENAME}"
# ```
ARG DEBIAN_VERSION_CODENAME=bullseye

# Add nonroot user
RUN useradd -ms /bin/bash nonroot -b /home
SHELL ["/bin/bash", "-c"]
