FROM swift:5.9 AS build
WORKDIR /source

COPY . .
RUN swift build --configuration release

FROM swift:5.9
WORKDIR /app
COPY --from=build /source/.build/release .
CMD ["/app/PostgresNIOExample"]
