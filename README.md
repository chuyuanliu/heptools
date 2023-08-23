# HEP Tools
Analysis tools for experimental high energy physics.

# Installation
## Conda (recommend [Mamba](https://mamba.readthedocs.io/) for performance)
Create an environment

    mamba env create -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/environment.yml
    conda activate heptools-dev
Install without dependencies

    pip install --no-dependencies git+https://github.com/chuyuanliu/heptools.git@master
## pip
Install all denpendencies with pip

    pip install git+https://github.com/chuyuanliu/heptools.git@master

# Run in container
A docker image will be automatically built and pushed to [docker hub](https://hub.docker.com/repository/docker/chuyuanliu/heptools) when a new commit is pushed to master branch.
## Run an ineractive shell with [Singularity(Apptainer)](https://apptainer.org/docs/user/latest/)

    singularity shell \
    -B .:/srv \
    -B /cvmfs \
    --pwd /srv \
    ${HEPTOOLS_DOCKER_IMAGE}
use image from [docker hub](https://hub.docker.com/repository/docker/chuyuanliu/heptools)

    export HEPTOOLS_DOCKER_IMAGE="docker://chuyuanliu/heptools:latest"
use image from [unpacked.cern.ch](https://cvmfs.readthedocs.io/en/latest/cpt-containers.html#using-unpacked-cern-ch) (recommended)

    export HEPTOOLS_DOCKER_IMAGE="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/chuyuanliu/heptools:latest"
## Submit jobs to batch system with [HTCondor](https://htcondor.readthedocs.io/)
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