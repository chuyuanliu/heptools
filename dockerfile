# syntax=docker/dockerfile:1
# slim https://github.com/CoffeaTeam/docker-coffea-base/blob/main/base/Dockerfile
FROM condaforge/mambaforge:latest

RUN mamba install --yes \
    -c conda-forge \
    python=3.9 \
    voms \
    xrootd \
    ca-policy-lcg \
    && mamba clean --all --yes \
    && pip install --no-cache-dir git+https://github.com/chuyuanliu/heptools.git@master
RUN ln -s /opt/conda/etc/grid-security /etc/grid-security