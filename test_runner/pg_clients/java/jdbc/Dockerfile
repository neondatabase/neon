FROM openjdk:17
WORKDIR /source

COPY . .

WORKDIR /app
RUN curl --output postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.4.0.jar && \
    javac -d /app /source/Example.java

CMD ["java", "-cp", "/app/postgresql.jar:.", "Example"]
