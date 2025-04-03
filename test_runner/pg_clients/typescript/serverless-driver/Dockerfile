# use base image node:22
FROM node:22@sha256:c7fd844945a76eeaa83cb372e4d289b4a30b478a1c80e16c685b62c54156285b
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
