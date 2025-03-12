# HEP Tools

[![Documentation](https://img.shields.io/badge/docs-latest-blue?logo=sphinx)](https://chuyuanliu.github.io/heptools/)
[![Docker Image](https://img.shields.io/badge/docker-latest-blue?logo=docker)](https://hub.docker.com/repository/docker/chuyuanliu/heptools)
![Python Version](https://img.shields.io/badge/python-3.11-blue?logo=python)
![Code Style](https://img.shields.io/badge/code%20style-black-black)

A collection of tools for experimental high energy physics analysis.

## Installation

### Conda (recommend [Mamba](https://mamba.readthedocs.io/) for performance)

Create an environment

```bash
mamba env create -f https://raw.githubusercontent.com/chuyuanliu/heptools/master/environment.yml
conda activate heptools-dev
```

Install without dependencies

```bash
pip install --no-dependencies git+https://github.com/chuyuanliu/heptools.git@master
```

### pip

Install all denpendencies with pip

```bash
pip install git+https://github.com/chuyuanliu/heptools.git@master
```

## Run in container

A docker image will be automatically built and pushed to [docker hub](https://hub.docker.com/repository/docker/chuyuanliu/heptools) when a new commit is pushed to master branch.

### Run an ineractive shell with [Singularity(Apptainer)](https://apptainer.org/docs/user/latest/)

```bash
singularity exec -B .:/srv -B /cvmfs --pwd /srv ${HEPTOOLS_DOCKER_IMAGE} bash --init-file /entrypoint.sh
```

use image from [docker hub](https://hub.docker.com/repository/docker/chuyuanliu/heptools)

```bash
export HEPTOOLS_DOCKER_IMAGE="docker://chuyuanliu/heptools:latest"
```

use image from [unpacked.cern.ch](https://cvmfs.readthedocs.io/en/latest/cpt-containers.html#using-unpacked-cern-ch) (recommended)

```bash
export HEPTOOLS_DOCKER_IMAGE="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/chuyuanliu/heptools:latest"
```

### Submit jobs to batch system with [HTCondor](https://htcondor.readthedocs.io/)

#### LPC

Create X.509 proxy. (stored in `${X509_USER_PROXY}`)

```bash
voms-proxy-init --rfc --voms cms -valid 192:00
```

Setup condor configuration. (stored in `${CONDOR_CONFIG}$`)

```bash
source <(curl -s https://raw.githubusercontent.com/chuyuanliu/heptools/master/tools/condor_config_lpc.sh) [<schedd_name>]
```

If `<schedd_name>` is provided, the default SCHEDD_HOST will be replaced. (check schedd status `condor_status -schedd`)

Start a container

```bash
singularity exec -B .:/srv -B /cvmfs -B /uscmst1b_scratch/lpc1 -B $(readlink ${HOME}/nobackup) --env "CONDOR_CONFIG=${CONDOR_CONFIG}" --pwd /srv ${HEPTOOLS_DOCKER_IMAGE} bash --init-file /entrypoint.sh
```

## Tips

- To change singularity (apptainer) cache and temp dir

```bash
export APPTAINER_CACHEDIR="new/path/to/cache/"
export APPTAINER_TMPDIR="new/path/to/tmp/"
```

- To run ml container in wsl:

```bash
singularity exec -B .:/srv -B /run/shm --nvccli --pwd /srv ${HEPTOOLS_DOCKER_IMAGE} bash --init-file /entrypoint.sh
```

## TODO

### Short term

- do not set module of customized exceptions `Exception.__module__`
- add a loose array for dask awkward

### New feature

- lightweight local database for `xs`, `dataset`

### General

- stable branch
- documentation
- comment
- unit test

### Compatibility

- Current:
  - use TYPE_CHECKING if possible
- Python 3.12: new generic type and typealias syntax
