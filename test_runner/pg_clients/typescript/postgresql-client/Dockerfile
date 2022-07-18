FROM node:16
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]