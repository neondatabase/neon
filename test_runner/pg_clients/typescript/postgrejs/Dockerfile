FROM node:23.11.0@sha256:c5bfe90b30e795ec57bcc0040065ca6f284af84a1dafd22a207bd6b48c39ce01
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
