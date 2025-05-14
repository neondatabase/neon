FROM python:3.13@sha256:8c55c44b9e81d537f8404d0000b7331863d134db87c1385dd0ec7fefff656495
WORKDIR /source

COPY . .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

CMD ["python3", "asyncpg_example.py"]
