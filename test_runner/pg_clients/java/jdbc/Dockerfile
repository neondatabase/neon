FROM openjdk:22
WORKDIR /source

COPY . .

WORKDIR /app
RUN curl --output postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.2.jar && \
    javac -d /app /source/Example.java

CMD ["java", "-cp", "/app/postgresql.jar:.", "Example"]
