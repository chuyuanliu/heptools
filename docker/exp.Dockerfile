FROM chuyuanliu/heptools:base

RUN pip install --no-cache-dir --no-dependencies git+https://github.com/chuyuanliu/heptools.git@master
ENTRYPOINT ["tini", "-g", "--"]