# use base image python:3.12
FROM python:3.12@sha256:4e7024df2f2099e87d0a41893c299230d2a974c3474e681b0996f141951f9817
WORKDIR /source

COPY . .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

CMD ["python3", "pg8000_example.py"]
