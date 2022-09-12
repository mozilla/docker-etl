FROM python:3
MAINTAINER Chelsea Troy <ctroy@mozilla.com>

WORKDIR /usr/app/src
WORKDIR /usr/app/

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

WORKDIR /usr/app/src/
