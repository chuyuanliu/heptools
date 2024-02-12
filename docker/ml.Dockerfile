FROM chuyuanliu/heptools:base

RUN mamba env update -n base -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/docker/ml.yml
ENTRYPOINT ["tini", "-g", "--"]