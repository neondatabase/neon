FROM swift:5.7 AS build
RUN apt-get -q update && apt-get -q install -y libssl-dev
WORKDIR /source

COPY . .
RUN swift build --configuration release

FROM swift:5.7
WORKDIR /app
COPY --from=build /source/.build/release .
CMD ["/app/PostgresClientKitExample"]
