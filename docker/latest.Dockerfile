FROM chuyuanliu/heptools:exp

RUN mamba install --yes \
    -c conda-forge \
    coffea=0.7.22 \
    && mamba clean --all --yes
ENTRYPOINT ["tini", "-g", "--"]