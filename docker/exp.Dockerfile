FROM chuyuanliu/heptools:base

RUN --mount=type=bind,target=/src \
    conda run -n hep pip install --no-cache-dir --no-dependencies /src
ENTRYPOINT ["tini", "-g", "--"]