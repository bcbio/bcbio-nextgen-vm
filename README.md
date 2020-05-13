## bcbio-nextgen-vm

[![Build Status](https://travis-ci.org/bcbio/bcbio-nextgen-vm.svg?branch=master)](https://travis-ci.org/bcbio/bcbio-nextgen-vm)

Run [bcbio-nextgen](https://github.com/bcbio/bcbio-nextgen) genomic sequencing analysis pipelines using code and tools on cloud platforms or isolated inside of lightweight containers. This enables:
* Improved installation: Pre-installing all required biological code, tools and system libraries inside a container removes the difficulties associated with supporting multiple platforms. Installation only requires setting up [Docker](https://www.docker.com/) and download of the latest container.
* Pipeline isolation: Third party software used in processing is fully isolated and will not impact existing tools or software. This eliminates the need for [Environment Modules](http://modules.sourceforge.net/) or PATH manipulation to provide partial isolation.
* Full reproducibility: You can maintain snapshots of the code and processing environment indefinitely, providing the ability to re-run an older analysis by reverting to an archived snapshot.

This currently supports running on [Amazon Web Services (AWS)](https://aws.amazon.com/) and locally with lightweight [Docker](https://www.docker.com/) containers. The bcbio documentation contains details on using [bcbio-vm to run analyses on AWS](https://bcbio-nextgen.readthedocs.io/en/latest/contents/cloud.html). We also have in progress work on migrating bcbio's pipeline descriptions to use the [Common Workflow Language (CWL)](https://github.com/bcbio/bcbio-nextgen/tree/master/bcbio/cwl).

We support using bcbio-vm for both AWS and local docker usage on Linux systems. On Mac OSX, only AWS usage currently works. Local docker support for Mac OSX is a work in progress and we have more details on the current status below. We welcome feedback and problem reports.

## Installation

* Install bcbio-vm using [Conda](https://docs.conda.io/en/latest/) with an isolated Miniconda Python and link to a location on your PATH:
    ```shell
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p ~/install/bcbio-vm/anaconda
    ~/install/bcbio-vm/anaconda/bin/conda install --yes -c conda-forge -c bioconda bcbio-nextgen
    ~/install/bcbio-vm/anaconda/bin/conda install --yes -c conda-forge -c bioconda bcbio-nextgen-vm
    ln -s ~/install/bcbio-vm/anaconda/bin/bcbio_vm.py /usr/local/bin/bcbio_vm.py
    ln -s ~/install/bcbio-vm/anaconda/bin/conda /usr/local/bin/bcbiovm_conda
    ```
    If you're using bcbio-vm from your local machine to run on a [pre-built remote AWS instance](https://bcbio-nextgen.readthedocs.io/en/latest/contents/cloud.html), or on an [Arvados cloud instance](https://bcbio-nextgen.readthedocs.io/en/latest/contents/cwl.html#running-on-arvados) this is all you need to get started. If you'd like to run locally or on a server with Docker, keep following the instructions to install the third party tools and data.
* [Install docker](https://docs.docker.com/engine/install/) on your system. You will need root permissions.
* [Setup a docker group](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user) to provide the ability to run Docker without being root. Some installations, like Debian/Ubuntu packages do this automatically. You'll also want to add the trusted user who will be managing and testing docker images to this group:
    ```shell
    sudo groupadd docker
    sudo service docker restart
    sudo gpasswd -a ${USERNAME} docker
    newgrp docker
    ```
* Ensure the driver script is [setgid](https://en.wikipedia.org/wiki/Setuid) to the docker group. This allows users to run bcbio-nextgen without needing to be in the docker group or have root access. To avoid security issues, `bcbio_vm.py` [sanitizes input arguments](https://github.com/bcbio/bcbio-nextgen-vm/blob/master/bcbiovm/docker/manage.py) and runs the internal docker process as the calling user using a [small wrapper script](https://github.com/bcbio/bcbio-nextgen-vm/blob/master/scripts/createsetuser) so it will only have permissions available to that user:
    ```shell
    sudo chgrp docker /usr/local/bin/bcbio_vm.py
    sudo chmod g+s /usr/local/bin/bcbio_vm.py
    ```
* Install a dockerized bcbio-nextgen. This will get the latest [bcbio docker image](https://github.com/bcbio/bcbio_docker) with software and tools, as well as downloading genome data:
    ```shell
    bcbio_vm.py --datadir=~/install/bcbio-vm/data install --data --tools \
      --genomes GRCh37 --aligners bwa
    ```
    For more details on expected download sizes, see the [bcbio system requirements](https://bcbio-nextgen.readthedocs.io/en/latest/contents/installation.html#system-requirements) documentation. By default, the installation will download and import the default docker image as `quay.io/bcbio/bcbio-vc`. You can specify an alternative image location with `--image your_image_name`, and skip the `--tools` argument if this image is already present and configured.

    If you have an existing bcbio-nextgen installation and want to avoid re-installing existing genome data, first symlink to the current installation data:
    ```shell
    mkdir ~/install/bcbio-vm/data
    cd ~/install/bcbio-vm/data
    ln -s /usr/local/share/bcbio_nextgen/genomes
    ln -s /usr/local/share/gemini/data gemini_data
    ```
* If you didn't use the recommended installation organization (a shared directory with code under `anaconda` and data under `data`) set the data location configuration once for each individual user of bcbio-nextgen to avoid needing to specify the location of data directories on subsequent runs:
    ```shell
    bcbio_vm.py --datadir=~/install/bcbio-vm/data saveconfig
    ```

## Running

Usage of bcbio_vm.py is similar to bcbio_nextgen.py, with some cleanups to make the command line more consistent. To run an analysis on a prepared bcbio-nextgen sample configuration file:
```shell
bcbio_vm.py run -n 4 sample_config.yaml
```
To run distributed on a cluster using IPython parallel:
```shell
bcbio_vm.py ipython sample_config.yaml torque your_queue -n 64
```
bcbio-nextgen also contains tests that exercise docker functionality:
```shell
cd bcbio-nextgen/tests
./run_tests.sh docker
./run_tests.sh docker_ipython
```

## Upgrading

bcbio-nextgen-vm enables easy updates of the wrapper code, tools and data. To update the wrapper code:
```shell
bcbio_vm.py install --wrapper
```
To update tools, with a download of the latest docker image:
```shell
bcbio_vm.py install --tools
```
To update the associated data files:
```shell
bcbio_vm.py install --data
```
Combine all commands to update everything concurrently.

## Development Notes

These notes are for building containers from scratch or developing on bcbio-nextgen.

### macOS

* Install [Git](https://git-scm.com/download/mac), [VirtualBox](https://download.virtualbox.org/virtualbox/6.1.6/VirtualBox-6.1.6-137129-OSX.dmg), and [Vagrant](https://releases.hashicorp.com/vagrant/2.2.9/vagrant_2.2.9_x86_64.dmg)
* Download bcbio-nextgen-vm and provision Vagrant VM:
    ```shell
    git clone git@github.com:bcbio/bcbio-nextgen-vm.git
    cd bcbio-nextgen-vm
    vagrant up
    ```
* Install bcbio-nextgen-vm:
    ```shell
    vagrant ssh
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p ~/bcbio-vm/anaconda
    conda install --yes -c conda-forge -c bioconda bcbio-nextgen-vm bcbio-nextgen
    ```
Optional steps:
* Inside the VM (`vagrant ssh`):
  * Set the time zone in the VM for easier log viewing, for example:
    ```shell
    sudo timedatectl set-timezone America/New_York
    ```
* Outside the VM:
  * To make any additional data from the host available inside the VM (for example: reference genomes, pipeline inputs, etc) set `BCBIO_DATA_DIR` environment variable on the host to a directory that contains the data, for example:
    ```shell
    export BCBIO_DATA_DIR=~/biodata
    vagrant reload
    ```
    This directory will be mounted inside Vagrant VM under `/data` 

### Docker image installation

Install the current bcbio docker image into your local repository by hand with:
```shell
docker pull quay.io/bcbio/bcbio-vc
```
The installer does this automatically, but this is useful if you want to work with the bcbio-nextgen docker image independently from the wrapper.

### Updates

To update bcbio-nextgen in a local docker instance during development, first clone the development code:
```shell
git clone https://github.com/chapmanb/bcbio-nextgen
cd bcbio-nextgen
```
Edit the code as needed, then update your local install with:
```shell
bcbio_vm.py devel setup_install
```
You can update the tools in your local container with:
```shell
bcbio_vm.py devel upgrade_tools
```
and register a GATK jar inside the container with:
```shell
bcbio_vm.py devel register gatk /path/to/GenomeAnalysisTK.tar.bz2
```

### Creating docker image

Docker hub builds the [bcbio docker image](https://github.com/bcbio/bcbio_docker). We manually trigger this build to avoid overloading Docker hub services with a long rebuild on every change to the bcbio repository.

### Preparing pre-built genomes

bcbio_vm downloads pre-built reference genomes when running analyses, to avoid needing these to be present on the initial machine images. To create the pre-built tarballs for a specific genome, start and bootstrap a single bcbio machine using the elasticluster interface. On the machine start a screen session then run:
```shell
bcbio_vm.py devel biodata --genomes GRCh37 --aligners bwa --aligners bowtie2 --datatarget vep
```
This requires permissions to write to the `biodata` bucket.
