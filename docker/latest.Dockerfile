FROM chuyuanliu/heptools:base

RUN mamba install --yes \
    -c conda-forge \
    coffea=0.7.22 \
    uproot=4.3.7 \
    awkward=1.10.3 \
    && pip install --no-cache-dir --no-dependencies git+https://github.com/chuyuanliu/heptools.git@master
ENTRYPOINT ["tini", "-g", "--"]