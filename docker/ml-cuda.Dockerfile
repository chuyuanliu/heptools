FROM chuyuanliu/heptools:base

ARG CUDA_VERSION

ENV CONDA_OVERRIDE_CUDA=${CUDA_VERSION}

RUN --mount=type=bind,source=docker/ml-cuda.yml,target=/tmp/ml-cuda.yml \
    mamba env update -n hep -f /tmp/ml-cuda.yml \
    && mamba clean --all --yes
