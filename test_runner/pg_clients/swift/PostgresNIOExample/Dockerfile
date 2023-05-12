FROM swift:5.8 AS build
WORKDIR /source

COPY . .
RUN swift build --configuration release

FROM swift:5.8
WORKDIR /app
COPY --from=build /source/.build/release .
CMD ["/app/PostgresNIOExample"]
