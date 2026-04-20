FROM chuyuanliu/heptools:base

ENV CONDA_OVERRIDE_CUDA="13.1"

RUN mamba env update -n hep -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/docker/ml.yml \
    && mamba clean --all --yes
ENTRYPOINT ["tini", "-g", "--"]