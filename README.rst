bcbio-nextgen-vm
----------------

Run `bcbio-nextgen`_ genomic sequencing analysis pipelines using code and tools
isolated inside of lightweight containers. This enables:

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

This currently supports lightweight `docker`_ containers. It is still a work in
progress and we welcome feedback and problem reports.

.. _bcbio-nextgen: https://github.com/chapmanb/bcbio-nextgen
.. _docker: http://www.docker.io/
.. _modules: http://modules.sourceforge.net/

Installation
------------

- `Install docker`_ on your system. You will need root permissions.

- `Setup a docker group`_ to provide the ability to run Docker without being
  root. You'll likely want to add the trusted user who will be managing and
  testing docker images to this group::

    sudo groupadd docker
    sudo gpasswd -a ${USERNAME} docker
    sudo service docker restart
    newgrp docker

- Install bcbio-nextgen-vm using `conda`_ with an isolated Python::

    wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
    bash Miniconda-latest-Linux-x86_64.sh -b -p ~/install/bcbio-vm/anaconda
    ~/install/bcbio-vm/anaconda/bin/conda install --yes -c https://conda.binstar.org/bcbio bcbio-nextgen-vm
    ln -s ~/install/bcbio-vm/anaconda/bin/bcbio_vm.py /usr/local/bin/bcbio_vm.py

  or with your system Python::

    pip install conda
    conda install -c https://conda.binstar.org/bcbio bcbio-nextgen-vm

- Ensure the driver script is `setgid`_ to the docker group. This allows users
  to run bcbio-nextgen without needing to be in the docker group or have root
  access. To avoid security issues, ``bcbio_vm.py`` `sanitizes input arguments`_
  and runs the internal docker process as the calling user using a
  `small wrapper script`_ so it will only have permissions available to
  that user::

    sudo chown :docker /usr/local/bin/bcbio_vm.py
    sudo chmod g+s /usr/local/bin/bcbio_vm.py

- Install bcbio-nextgen. This will get the latest `bcbio-nextgen docker index`_
  with software and tools, as well as downloading genome data::

    bcbio_vm.py --datadir=~/install/bcbio-vm/data install --data --tools

  For more details on expected download sizes, see the `bcbio system
  requirements`_ documentation. By default, the installation will download and
  import the default docker image as ``chapmanb/bcbio-nextgen-devel``. You can
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
.. _Docker index: https://index.docker.io/
.. _bcbio-nextgen docker index: https://index.docker.io/u/chapmanb/bcbio-nextgen-devel/
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

ToDo
====

- Enable specification of external programs/jars to handle tricky non-distributable
  issues like GATK protected versions. Map these directories into docker
  container.
- Improve ability to develop and test on locally installed images.

Creating docker image
=====================

An `ansible <http://www.ansible.com>`_ playbook automates the process of
creating the bcbio-nextgen docker images. To build on AWS and upload the latest
image to S3::

    cd ansible
    vim defaults.yml
    ansible-playbook bcbio_vm_aws.yml --extra-vars "@defaults.yml"

or locally, with Docker pre-installed::

    ansible-playbook -c local bcbio_vm_local.yml --extra-vars "@defaults.yml"

Docker image installation
=========================

Install the current bcbio docker image into your local repository by hand with::

    docker import https://s3.amazonaws.com/bcbio_nextgen/bcbio-nextgen-docker-image.gz chapmanb/bcbio-nextgen-devel

The installer does this automatically, but this is useful if you want to work
with the bcbio-nextgen docker image independently from the wrapper.

Updates
=======

Update local images during development::

    DID=$(docker run -d -i -t -v ~/bio/bcbio-nextgen:/tmp/bcbio-nextgen
          chapmanb/bcbio-nextgen-devel /bin/bash)
    docker attach $DID
    cd /tmp/bcbio-nextgen
    /usr/local/share/bcbio-nextgen/anaconda/bin/python setup.py install
    docker commit $DID chapmanb/bcbio-nextgen-devel
