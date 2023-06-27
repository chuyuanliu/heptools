# HEP Tools
Tools for high energy physics analysis.

# Installation
install from github

    pip install git+https://github.com/ChuyuanLiu/heptools.git@master
or from local

    pip install .
# Build image and run in container
## `docker`
build docker image

    docker build -t [TAG] https://github.com/ChuyuanLiu/heptools.git#master
## `singularity`
run an interactive shell

    singularity shell                   `# run a shell within a container`\
    -B /cvmfs                           `# mount cvmfs`\
    -B ~/nobackup:/nobackup             `# mount nobackup dir`\
    -B .:/analysis                      `# mount analysis dir`\
    --pwd /analysis                     `# set initial working directory`\
    docker://chuyuanliu/heptools:latest `# use prebuilt docker image`
## Tips
- change singularity (apptainer) cache dir `export APPTAINER_CACHEDIR=${HOME}/nobackup/.apptainer/`

# TODO
- condor
- plot
- dasgoclient