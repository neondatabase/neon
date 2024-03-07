FROM node:21
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
