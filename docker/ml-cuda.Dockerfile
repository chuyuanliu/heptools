FROM chuyuanliu/heptools:base

ARG CUDA_VERSION

ENV CONDA_OVERRIDE_CUDA=${CUDA_VERSION}

RUN mamba env update -n hep -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/docker/ml.yml \
    && mamba clean --all --yes
ENTRYPOINT ["tini", "-g", "--"]