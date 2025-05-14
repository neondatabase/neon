FROM node:23@sha256:047d633b358c33f900110efff70b4f1c73d066dec92dd6941c42d26889f69a0e
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
