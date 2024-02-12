FROM condaforge/mambaforge:latest

RUN mamba env update -n base -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/docker/base.yml \
    && mamba install --yes \
    -c conda-forge \
    # grid certificate
    voms \
    ca-policy-lcg \
    # XRootD
    xrootd \
    fsspec-xrootd \
    # tini
    tini \
    && mamba clean --all --yes
RUN ln -s /opt/conda/etc/grid-security /etc/grid-security
ENTRYPOINT ["tini", "-g", "--"]