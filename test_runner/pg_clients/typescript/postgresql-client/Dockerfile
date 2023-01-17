FROM node:18
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
