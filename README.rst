bcbio-nextgen-vm
----------------

Run `bcbio-nextgen`_ genomic sequencing analysis pipelines using code and tools
on cloud platforms or isolated inside of lightweight containers. This enables:

- Improved installation: Pre-installing all required biological code, tools and
  system libraries inside a container removes the difficulties associated with
  supporting multiple platforms. Installation only requires setting up
  `docker`_ and download of the latest container.

- Pipeline isolation: Third party software used in processing is fully isolated
  and will not impact existing tools or software. This eliminates the need for
  `modules`_ or PATH manipulation to provide partial isolation.

- Full reproducibility: You can maintain snapshots of the code and processing
  environment indefinitely, providing the ability to re-run an older analysis
  by reverting to an archived snapshot.

This currently supports running on `Amazon Web Services (AWS)
<http://aws.amazon.com/>`_ and locally with lightweight `docker`_ containers.
The bcbio documentation contains details on using `bcbio-vm to run analyses on AWS
<https://bcbio-nextgen.readthedocs.org/en/latest/contents/cloud.html>`_. We
also have in progress work on migrating bcbio's pipeline descriptions to use the
`Common Workflow Language (CWL) <https://github.com/chapmanb/bcbio-nextgen/tree/master/cwl>`_.

We support using bcbio-vm for both AWS and local docker usage on Linux
systems. On Mac OSX, only AWS usage currently works. Local docker support for
Mac OSX is a work in progress and we have more details on the current status below.
We welcome feedback and problem reports.

.. _bcbio-nextgen: https://github.com/chapmanb/bcbio-nextgen
.. _docker: http://www.docker.io/
.. _modules: http://modules.sourceforge.net/

Installation
------------

- Install bcbio-vm using `conda`_ with an isolated Miniconda Python and link to
  a location on your PATH::

     wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
     bash Miniconda-latest-Linux-x86_64.sh -b -p ~/install/bcbio-vm/anaconda
     ~/install/bcbio-vm/anaconda/bin/conda install --yes -c conda-forge -c bioconda bcbio-nextgen-vm
     ln -s ~/install/bcbio-vm/anaconda/bin/bcbio_vm.py /usr/local/bin/bcbio_vm.py
     ln -s ~/install/bcbio-vm/anaconda/bin/arvados-cwl-runner /usr/local/bin/arvados-cwl-runner
     ln -s ~/install/bcbio-vm/anaconda/bin/cwltool /usr/local/bin/cwltool
     ln -s ~/install/bcbio-vm/anaconda/bin/conda /usr/local/bin/bcbiovm_conda

  If you're using bcbio-vm from your local machine to run on a `pre-built
  remote AWS instance <https://bcbio-nextgen.readthedocs.org/en/latest/contents/cloud.html>`_,
  or on an `Arvados cloud instance
  <https://github.com/chapmanb/bcbio-nextgen/tree/master/cwl#running-bcbio-cwl-on-arvados>`_
  this is all you need to get started. If you'd like to run locally or on a
  server with Docker, keep following the instructions to install the third party
  tools and data.

- `Install docker`_ on your system. You will need root permissions.

- `Setup a docker group`_ to provide the ability to run Docker without being
  root. Some installations, like Debian/Ubuntu packages do this automatically.
  You'll also want to add the trusted user who will be managing and
  testing docker images to this group::

    sudo groupadd docker
    sudo service docker restart
    sudo gpasswd -a ${USERNAME} docker
    newgrp docker

- Ensure the driver script is `setgid`_ to the docker group. This allows users
  to run bcbio-nextgen without needing to be in the docker group or have root
  access. To avoid security issues, ``bcbio_vm.py`` `sanitizes input arguments`_
  and runs the internal docker process as the calling user using a
  `small wrapper script`_ so it will only have permissions available to
  that user::

    sudo chgrp docker /usr/local/bin/bcbio_vm.py
    sudo chmod g+s /usr/local/bin/bcbio_vm.py

- Install a dockerized bcbio-nextgen. This will get the latest `bcbio docker image`_
  with software and tools, as well as downloading genome data::

    bcbio_vm.py --datadir=~/install/bcbio-vm/data install --data --tools \
      --genomes GRCh37 --aligners bwa

  For more details on expected download sizes, see the `bcbio system
  requirements`_ documentation. By default, the installation will download and
  import the default docker image as ``bcbio/bcbio``. You can
  specify an alternative image location with ``--image your_image_name``, and
  skip the ``--tools`` argument if this image is already present and configured.

  If you have an existing bcbio-nextgen installation and want to avoid
  re-installing existing genome data, first symlink to the current installation
  data::

    mkdir ~/install/bcbio-vm/data
    cd ~/install/bcbio-vm/data
    ln -s /usr/local/share/bcbio_nextgen/genomes
    ln -s /usr/local/share/gemini/data gemini_data

- If you didn't use the recommended installation organization (a shared
  directory with code under ``anaconda`` and data under ``data``) set the data
  location configuration once for each individual user of bcbio-nextgen to avoid
  needing to specify the location of data directories on subsequent runs::

    bcbio_vm.py --datadir=~/install/bcbio-vm/data saveconfig

.. _Install docker: http://docs.docker.io/en/latest/installation/#installation-list
.. _Setup a docker group: http://docs.docker.io/en/latest/use/basics/#dockergroup
.. _bcbio docker image: https://hub.docker.com/r/bcbio/bcbio/
.. _setgid: https://en.wikipedia.org/wiki/Setuid
.. _conda: http://conda.pydata.org/
.. _sanitizes input arguments: https://github.com/chapmanb/bcbio-nextgen-vm/blob/master/bcbiovm/docker/manage.py
.. _small wrapper script: https://github.com/chapmanb/bcbio-nextgen-vm/blob/master/scripts/createsetuser
.. _bcbio system requirements: https://bcbio-nextgen.readthedocs.org/en/latest/contents/installation.html#system-requirements

Running
-------

Usage of bcbio_vm.py is similar to bcbio_nextgen.py, with some
cleanups to make the command line more consistent. To run an analysis on a
prepared bcbio-nextgen sample configuration file::

  bcbio_vm.py run -n 4 sample_config.yaml

To run distributed on a cluster using IPython parallel::

  bcbio_vm.py ipython sample_config.yaml torque your_queue -n 64

bcbio-nextgen also contains tests that exercise docker functionality::

  cd bcbio-nextgen/tests
  ./run_tests.sh docker
  ./run_tests.sh docker_ipython

Upgrading
---------

bcbio-nextgen-vm enables easy updates of the wrapper code, tools and data. To
update the wrapper code::

    bcbio_vm.py install --wrapper

To update tools, with a download of the latest docker image::

    bcbio_vm.py install --tools

To update the associated data files::

    bcbio_vm.py install --data

Combine all commands to update everything concurrently.

Development Notes
-----------------

These notes are for building containers from scratch or developing on
bcbio-nextgen.

Mac OSX docker support
======================

Running Docker on Mac OSX requires using a virtual machine wrapper. The
`recommended approach <https://docs.docker.com/installation/mac/>`_ is to use
`boot2docker <https://github.com/boot2docker/boot2docker>`_ which wraps docker
inside VirtualBox.

The current issue is mounting external directories into boot2docker. The mounts
work as of `Docker 1.3
<http://blog.docker.com/2014/10/docker-1-3-signed-images-process-injection-security-options-mac-shared-directories/>`_,
but do not maintain the original user ID and group ID, but rather get mounted as
root. Since bcbio runs as the original user to avoid security issues, you don't have
permissions to make modifications in the directories. There is an `open issue on
the problem <https://github.com/boot2docker/boot2docker/issues/581>`_ and we're
currently not sure about the best approach or workaround.

Also, if you experience timeouts while pulling the docker image on OSX, please try
to reboot the VirtualBox VM running boot2docker and/or upgrade it via:

`docker-machine upgrade <boot2docker_VM>`

We'd be happy to accept patches/suggestions from interested Mac OSX users.

Docker image installation
=========================

Install the current bcbio docker image into your local repository by hand with::

    docker pull bcbio/bcbio

The installer does this automatically, but this is useful if you want to work
with the bcbio-nextgen docker image independently from the wrapper.

Updates
=======

To update bcbio-nextgen in a local docker instance during development, first
clone the development code::

    git clone https://github.com/chapmanb/bcbio-nextgen
    cd bcbio-nextgen

Edit the code as needed, then update your local install with::

    bcbio_vm.py devel setup_install

You can update the tools in your local container with::

    bcbio_vm.py devel upgrade_tools

and register a GATK jar inside the container with::

    bcbio_vm.py devel register gatk /path/to/GenomeAnalysisTK.tar.bz2

Creating docker image
=====================

Docker hub builds the `bcbio docker image`_. We manually trigger this build
to avoid overloading Docker hub services with a long rebuild on every change to
the bcbio repository.

Preparing pre-built genomes
===========================

bcbio_vm downloads pre-built reference genomes when running analyses, to avoid
needing these to be present on the initial machine images. To create the
pre-built tarballs for a specific genome, start and bootstrap a single bcbio
machine using the elasticluster interface. On the machine start a screen session
then run::

   bcbio_vm.py devel biodata --genomes GRCh37 --aligners bwa --aligners bowtie2 --datatarget vep

This requires permissions to write to the ``biodata`` bucket.
