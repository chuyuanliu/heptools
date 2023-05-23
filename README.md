# HEP Tools
Tools for high energy physics analysis.

# Getting Started
## `pip`
install from github

    pip install git+https://github.com/ChuyuanLiu/heptools@master
or from local

    pip install .
## `conda`
use pip
## Container
### `docker`
build docker image

    docker build -t <image> .
run script in container

    docker run -it -rm \
    -v <script dir>:/analysis \
    <image> python <script>

### `singularity`
run script in singularity container

    singularity run \
    -B <script dir>:/analysis \
    <image> python /analysis/<script>

### convert image
save docker image to tar archive

    docker save <image> > <archive>.tar
build singularity image from docker archive

    singularity build <image>.sif docker-archive://<archive>.tar

### Tips
- pre-built image is avaliable in docker hub `docker://chuyuanliu/heptools:latest`
- mount `/cvmfs` in `singularity`: add `-B /cvmfs:/cvmfs:ro`

# TODO
- plot
- save skim