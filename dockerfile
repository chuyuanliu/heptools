# syntax=docker/dockerfile:1
FROM python:3.9-slim

RUN pip3 install git+https://github.com/ChuyuanLiu/heptools@master
WORKDIR /analysis