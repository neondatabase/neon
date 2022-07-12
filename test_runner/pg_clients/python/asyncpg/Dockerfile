FROM python:3.10
WORKDIR /source

COPY . .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

CMD ["python3", "asyncpg_example.py"]
