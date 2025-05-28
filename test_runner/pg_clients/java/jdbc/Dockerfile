FROM openjdk:23@sha256:fd0f3721ad4abbfe393ac1a2672abf4b7f53b5ecd9803a4e2444aa3acd7d398e
WORKDIR /source

COPY . .

WORKDIR /app
RUN curl --output postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    javac -d /app /source/Example.java

CMD ["java", "-cp", "/app/postgresql.jar:.", "Example"]
