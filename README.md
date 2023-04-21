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

    docker build --tag <image name> .
run script in container

    docker run -it -rm \
    -v <script dir>:/analysis \
    <image name> python <script>.py
