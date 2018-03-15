FROM ryanmwhitephd/docker-alpine-python:latest

MAINTAINER Ryan White <ryan.white4@canada.ca>

RUN pip install python-Levenshtein

ADD record-linkage ./record-linkage
