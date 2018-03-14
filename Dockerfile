FROM python:3.6.4-alpine3.7

MAINTAINER Ryan White <ryan.white4@canada.ca>

RUN apk add --update gcc g++ libc-dev && rm -rf /var/cache/apk/*

RUN pip install pypachy

RUN pip install python-Levenshtein

ADD record-linkage ./record-linkage
