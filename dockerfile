# syntax=docker/dockerfile:1
FROM python:3.9-slim

WORKDIR /heptools
COPY . .
RUN pip3 install .

WORKDIR /analysis