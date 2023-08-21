# HEP Tools
Analysis tools for experimental high energy physics.

# Installation
Install from github

    pip install git+https://github.com/chuyuanliu/heptools.git@master
# Build image and run in container
## [Docker](https://docs.docker.com/get-started/overview/)
Build docker image

    docker build -t <tag> https://github.com/chuyuanliu/heptools.git#master
## [Singularity](https://apptainer.org/docs/user/main/index.html)
Run an interactive shell

    singularity shell \
    -B .:/srv \
    -B /cvmfs \
    --pwd /srv \
    ${HEPTOOLS_DOCKER_IMAGE}
using [docker hub](https://hub.docker.com/repository/docker/chuyuanliu/heptools)

    export HEPTOOLS_DOCKER_IMAGE="docker://chuyuanliu/heptools:latest"
or [unpacked.cern.ch](https://cvmfs.readthedocs.io/en/latest/cpt-containers.html#using-unpacked-cern-ch) (recommended)

    export HEPTOOLS_DOCKER_IMAGE="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/chuyuanliu/heptools:latest"
## [HTCondor](https://htcondor.readthedocs.io/en/latest/)
### LPC
Create X.509 proxy. (stored in `${X509_USER_PROXY}`)

    voms-proxy-init --rfc --voms cms -valid 192:00

Setup condor configuration. (stored in `${CONDOR_CONFIG}$`)

    source <(curl -s https://raw.githubusercontent.com/chuyuanliu/heptools/master/tools/condor_config_lpc.sh) [<schedd_name>]
If `<schedd_name>` is provided, the default SCHEDD_HOST will be replaced. (check schedd status `condor_status -schedd`)

Start a container

    singularity shell \
    -B .:/srv \
    -B /cvmfs \
    -B /uscmst1b_scratch/lpc1/3DayLifetime \
    -B $(readlink ${HOME}/nobackup) \
    --env "CONDOR_CONFIG=${CONDOR_CONFIG}" \
    --pwd /srv \
    ${HEPTOOLS_DOCKER_IMAGE}

## Tips
change singularity (apptainer) cache dir

    export APPTAINER_CACHEDIR="${HOME}/nobackup/.apptainer/"

# TODO
- doc
- unit test
- plot
- dasgoclient