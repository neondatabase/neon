FROM node:20
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
