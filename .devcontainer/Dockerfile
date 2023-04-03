FROM condaforge/mambaforge:latest
LABEL Description="B2Find dev container" Vendor="DKRZ" Version="0.1"

RUN \
  apt-get update && \
  apt-get -y install \
          build-essential \
          bash-completion \
          vim \
          unzip \
          curl \
          wget \
          git \
          byobu

COPY .bashrc /root/.bashrc

WORKDIR /workspaces

RUN conda init bash

CMD ["/bin/bash"]
