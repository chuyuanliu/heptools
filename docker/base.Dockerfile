FROM condaforge/mambaforge:latest

RUN mamba env update -n base -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/docker/base.yml \
    && mamba install --yes \
    -c conda-forge \
    # grid certificate
    voms \
    ca-policy-lcg \
    # HTCondor
    htcondor \
    # XRootD
    xrootd \
    fsspec-xrootd \
    # tini
    tini \
    && mamba clean --all --yes \
    && pip install --no-cache-dir \
    # DB
    dbs3-client \
    rucio-clients
RUN ln -s /opt/conda/etc/grid-security /etc/grid-security
# rucio
RUN mkdir -p /opt/rucio/etc/
RUN wget -O /opt/rucio/etc/rucio.cfg https://raw.githubusercontent.com/dmwm/CMSRucio/master/docker/CMSRucioClient/rucio-prod.cfg
ENTRYPOINT ["tini", "-g", "--"]