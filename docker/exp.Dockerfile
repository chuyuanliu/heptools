FROM chuyuanliu/heptools:base

RUN --mount=type=bind,target=/repo,rw=true \
    conda run -n hep pip install --no-cache-dir --no-dependencies /repo
