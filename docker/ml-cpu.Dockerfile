FROM chuyuanliu/heptools:base

RUN --mount=type=bind,source=docker/ml-cpu.yml,target=/tmp/ml-cpu.yml \
    mamba env update -n hep -f /tmp/ml-cpu.yml \
    && mamba clean --all --yes
ENTRYPOINT ["tini", "-g", "--"]