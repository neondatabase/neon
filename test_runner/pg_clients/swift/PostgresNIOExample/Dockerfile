FROM swift:6.1@sha256:8bc5c78c1fb8ce2288fdffcf882941dd95402457ed7adf00ff628df84ddc7e93 AS build
WORKDIR /source

COPY . .
RUN swift build --configuration release

FROM swift:6.1@sha256:8bc5c78c1fb8ce2288fdffcf882941dd95402457ed7adf00ff628df84ddc7e93
WORKDIR /app
COPY --from=build /source/.build/release .
CMD ["/app/PostgresNIOExample"]
