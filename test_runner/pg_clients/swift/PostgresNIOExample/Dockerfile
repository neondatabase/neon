# use base image swift:5.10
FROM swift:5.10@sha256:69828d795abc6aa3f7b7f7a02054ab308420f54dcc806fcbd89c54944e61a089 AS build
WORKDIR /source

COPY . .
RUN swift build --configuration release

# use base image swift:5.10
FROM swift:5.10@sha256:69828d795abc6aa3f7b7f7a02054ab308420f54dcc806fcbd89c54944e61a089
WORKDIR /app
COPY --from=build /source/.build/release .
CMD ["/app/PostgresNIOExample"]
