# HEP Tools
Tools for high energy physics analysis.

# Getting Started
## pip
install from pip

    pip install git+https://github.com/ChuyuanLiu/heptools@master
or

    pip install .
## docker
build docker image

    docker build -t <image> .
run script in container

    docker run -it -rm \
    -v <script dir>:/analysis \
    <image> python <script>.py

save docker image to tar archive

    docker save <image> > <archive>.tar
## singularity
build singularity image from docker archive

    singularity build <image>.sif docker-archive://<archive>.tar

run script in singularity container

    singularity run \
    -B <script dir>:/analysis \
    <image>.sif python /analysis/<script>.py