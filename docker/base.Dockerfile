FROM condaforge/mambaforge:22.11.1-4

RUN mamba create -n hep -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/docker/base.yml \
    && mamba install --yes \
    -c conda-forge \
    # grid certificate
    ca-policy-lcg \
    # HTCondor
    # htcondor \
    # XRootD
    xrootd \
    fsspec-xrootd \
    # tini
    tini \
    && mamba clean --all --yes \
    && pip install --no-cache-dir \
    # DB
    dbs3-client \
    rucio-clients
RUN touch /root/.rnd
RUN ln -s /opt/conda/etc/grid-security /etc/grid-security
RUN apt-get update && apt-get install -y --no-install-recommends \
    # voms
    voms-clients-java \
    # bash tools
    bash-completion \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
# voms https://twiki.cern.ch/twiki/bin/view/LCG/VOMSLSCfileConfiguration
## Deprecated:
## voms2.cern.ch
## lcg-voms2.cern.ch
COPY <<EOF /etc/vomses
"cms" "voms-cms-auth.app.cern.ch" "443" "/DC=ch/DC=cern/OU=computers/CN=cms-auth.web.cern.ch" "cms"
EOF
COPY <<EOF /etc/grid-security/vomsdir/cms/voms-cms-auth.app.cern.ch.lsc
/DC=ch/DC=cern/OU=computers/CN=cms-auth.web.cern.ch
/DC=ch/DC=cern/CN=CERN Grid Certification Authority
EOF
ENV VOMS_PROXY_INIT_DONT_VERIFY_AC=1
# rucio
RUN mkdir -p /opt/rucio/etc/
RUN wget -O /opt/rucio/etc/rucio.cfg https://raw.githubusercontent.com/dmwm/CMSRucio/820e1ab3235e9ef0d97671b7da14c8c489d08fb5/docker/rucio_client/rucio-prod.cfg
# bashrc
RUN echo "conda activate hep" >> ~/.bashrc
# entrypoint
ENTRYPOINT ["tini", "-g", "--"]
