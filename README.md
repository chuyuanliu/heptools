# HEP Tools

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
singularity shell -B .:/srv -B /cvmfs --pwd /srv ${HEPTOOLS_DOCKER_IMAGE}
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
singularity shell -B .:/srv -B /cvmfs -B /uscmst1b_scratch/lpc1 -B $(readlink ${HOME}/nobackup) --env "CONDOR_CONFIG=${CONDOR_CONFIG}" --pwd /srv ${HEPTOOLS_DOCKER_IMAGE}
```

## Tips

change singularity (apptainer) cache dir

```bash
export APPTAINER_CACHEDIR="${HOME}/nobackup/.apptainer/"
```

change singularity (apptainer) temp dir

```bash
export APPTAINER_TMPDIR="${HOME}/nobackup/.apptainer/"
```

## TODO

- check: use TYPE_CHECKING to avoid circular import, unused import
- add:friend tree
- add:hist utils
- add:hist visualizer
- rewrite: XSection
- comment
- unit test
