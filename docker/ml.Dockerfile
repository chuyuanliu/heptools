FROM chuyuanliu/heptools:base

RUN mamba env update -n base -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/docker/ml.yml \
    && mamba clean --all --yes
ENTRYPOINT ["tini", "-g", "--"]