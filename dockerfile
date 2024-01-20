# syntax=docker/dockerfile:1
# https://github.com/CoffeaTeam/docker-coffea-base/blob/main/base/Dockerfile
# https://github.com/CoffeaTeam/docker-coffea-dask/blob/main/dask/Dockerfile
FROM condaforge/mambaforge:latest

RUN mamba env update -n base -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/main.yml \
    && mamba install --yes \
    -c conda-forge \
    # grid certificate
    voms \
    ca-policy-lcg \
    # XRootD
    xrootd \
    # HTCondor
    htcondor \
    # tini
    tini \
    && mamba clean --all --yes \
    && pip install --no-cache-dir --no-dependencies git+https://github.com/chuyuanliu/heptools.git@master
RUN ln -s /opt/conda/etc/grid-security /etc/grid-security
# rucio
RUN mkdir -p /opt/rucio/etc/
RUN wget -O /opt/rucio/etc/rucio.cfg https://raw.githubusercontent.com/dmwm/CMSRucio/master/docker/CMSRucioClient/rucio-prod.cfg
ENTRYPOINT ["tini", "-g", "--"]