FROM condaforge/mambaforge:latest
# Specify maintainer in case you share your Docker Image, like, e.g.
# MAINTAINER FirstName LastName <email>
LABEL Description="Docker4You Demo" Vendor="DKRZ" Version="0.1"

RUN \
  apt-get update && \
  apt-get -y install \
          build-essential \
          vim \
          unzip \
          curl \
          wget \
          git \
          byobu

COPY .bashrc /root/.bashrc

RUN \
  git config --global user.email "heil@dkrz.de" && \
  git config --global user.name "atmodatcode"

WORKDIR /opt

RUN conda init bash

CMD ["/bin/bash"]
