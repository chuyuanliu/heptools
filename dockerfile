# syntax=docker/dockerfile:1
# https://github.com/CoffeaTeam/docker-coffea-base/blob/main/base/Dockerfile
# https://github.com/CoffeaTeam/docker-coffea-dask/blob/main/dask/Dockerfile
FROM condaforge/mambaforge:latest

RUN mamba install --yes \
    -c conda-forge \
    python=3.9 \
    voms \
    xrootd \
    ca-policy-lcg \
    dask-jobqueue\
    && mamba clean --all --yes \
    && pip install --no-cache-dir git+https://github.com/chuyuanliu/heptools.git@master
RUN ln -s /opt/conda/etc/grid-security /etc/grid-security