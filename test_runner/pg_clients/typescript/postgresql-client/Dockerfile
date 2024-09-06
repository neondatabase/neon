FROM node:22
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
