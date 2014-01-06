bcbio-nextgen-vm
----------------

Run `bcbio-nextgen`_ genomic sequencing analysis pipelines using code and tools
isolated inside of lightweight containers and virtual machines. This enables:

- Improved installation: Pre-installing all required biological code, tools and
  system libraries inside a container removes the difficulties associated with
  supporting multiple platforms. Installation only requires setting up the
  virtual environment and download of the latest container.

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

- Install bcbio-nextgen-vm (will move to a pip install)::

    git clone https://github.com/chapmanb/bcbio-nextgen-vm
    cd bcbio-nextgen-vm
    virtualenv (or pyvenv) venv
    ./venv/bin/pip install distribute
    ./venv/bin/python setup.py install

- Ensure the driver script is `setgid`_ to the docker group. This allows users
  to run bcbio-nextgen without needing to be in the docker group or have root
  access. To avoid security issues, ``bcbio_nextgen_docker.py`` starts the
  internal docker process as the calling user so it will only have permissions
  available to the that user::

    sudo chown :docker ./venv/bin/bcbio_nextgen_docker.py
    sudo chmod g+s ./venv/bin/bcbio_nextgen_docker.py
    ln -s `pwd`/venv/bin/bcbio_nextgen_docker.py /usr/local/bin

- Install bcbio-nextgen. This will get the latest `bcbio-nextgen docker index`_
  with software and tools, as well as downloading genome data::

    bcbio_nextgen_docker.py --datadir=~/install/bcbio-nextgen-docker install --data --tools

  If you have an existing bcbio-nextgen installation and want to avoid
  reinstalling existing genome data, symlink to the current installation data::

    mkdir ~/install/bcbio-nextgen-docker
    cd ~/install/bcbio-nextgen-docker
    ln -s /usr/local/share/bcbio_nextgen/genomes
    ln -s /usr/local/share/gemini/data gemini_data

- To avoid needing to specify the location of data directories, set the
  data location configuration once for each new user::

    bcbio_nextgen_docker.py --datadir=~/install/bcbio-nextgen-docker saveconfig

.. _Install docker: http://docs.docker.io/en/latest/installation/#installation-list
.. _Setup a docker group: http://docs.docker.io/en/latest/use/basics/#dockergroup
.. _Docker index: https://index.docker.io/
.. _bcbio-nextgen docker index: https://index.docker.io/u/chapmanb/bcbio-nextgen-devel/
.. _setgid: https://en.wikipedia.org/wiki/Setuid

Running
-------

Usage of bcbio_nextgen_docker.py is similar to bcbio_nextgen.py, with some
cleanups to make the command line more consistent. To run an analysis on a
prepared bcbio-nextgen sample configuration file::

  bcbio_nextgen_docker.py run -n 1 sample_config.yaml

bcbio-nextgen also contains tests to exercise docker functionality::

  cd bcbio-nextgen/tests
  ./run_tests.sh docker

Development Notes
-----------------

These notes are for building containers from scratch or developing on
bcbio-nextgen.

ToDo
====

- Add full instructions for running test scripts.
- Finalize single machine, multicore runs of bcbio-nextgen with docker
  containers. Handle hanging at the end of multicore runs.
- Improve docker installation size: combine bcbio-nextgen and gemini anaconda
  directories. Load snpEff databases with genome data.
- Enable specification of external programs/jars to handle tricky non-distributable
  issues like GATK protected versions. Map these directories into docker container.
- Provide IPython/ZeroMQ interface that handles container creation and running
  of processes, passing actual execution to docker container.

Creating containers
===================

Start up docker::

    DID=$(docker run -d -i -t -p 8085:8085 stackbrew/ubuntu:13.10 /bin/bash)
    docker attach $DID

Install bcbio-nextgen via instructions in Dockerfile. Then commit::

    docker commit $DID chapmanb/bcbio-nextgen-devel

or build directly::

    docker build -t chapmanb/bcbio-nextgen-devel .

Updates
=======

Upload local images to `Docker index`_::

    DID=$(docker run -d -i -t chapmanb/bcbio-nextgen-devel /bin/bash)
    DID=$(docker run -d -i -t -p 8085:8085 -v ~/bio/bcbio-nextgen:/tmp/bcbio-nextgen
          -v /usr/local/share/bcbio_nextgen:/mnt/biodata
          chapmanb/bcbio-nextgen-devel /bin/bash)
    docker attach $DID
    docker commit $DID chapmanb/bcbio-nextgen-devel
    docker push chapmanb/bcbio-nextgen-devel

Update and test local code::

    docker attach bcbio-develrepo
    cd /tmp/bcbio-nextgen
    /usr/local/share/bcbio-nextgen/anaconda/bin/python setup.py install
    bcbio_nextgen.py server --port=8085
