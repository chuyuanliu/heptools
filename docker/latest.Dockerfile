FROM chuyuanliu/heptools:base

RUN mamba remove --yes \ 
    awkward-pandas \
    dask-awkward \
    && mamba install --yes \
    -c conda-forge \
    coffea=0.7.22 \
    && mamba clean --all --yes \
    && pip install --no-cache-dir --no-dependencies git+https://github.com/chuyuanliu/heptools.git@master
ENTRYPOINT ["tini", "-g", "--"]