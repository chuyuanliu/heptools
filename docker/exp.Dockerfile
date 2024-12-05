FROM chuyuanliu/heptools:base

RUN conda run -n hep pip install --no-cache-dir --no-dependencies git+https://github.com/chuyuanliu/heptools.git@master
ENTRYPOINT ["tini", "-g", "--"]