FROM python:3.11

ADD . /app
WORKDIR /app

RUN apt-get update
RUN apt-get install -y --no-install-recommends g++ protobuf-compiler libprotobuf-dev

RUN pip install -r requirements.txt
