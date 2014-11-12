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
  root. Some installations, like Debian/Ubuntu packages do this automatically.
  You'll also want to add the trusted user who will be managing and
  testing docker images to this group::

    sudo groupadd docker
    sudo service docker restart
    sudo gpasswd -a ${USERNAME} docker
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

    sudo chgrp docker /usr/local/bin/bcbio_vm.py
    sudo chmod g+s /usr/local/bin/bcbio_vm.py

- Install bcbio-nextgen. This will get the latest `bcbio-nextgen docker index`_
  with software and tools, as well as downloading genome data::

    bcbio_vm.py --datadir=~/install/bcbio-vm/data install --data --tools \
      --genomes GRCh37 --aligners bwa

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

Running on Amazon Web Services (AWS)
------------------------------------

bcbio uses `Elasticluster <https://github.com/gc3-uzh-ch/elasticluster>`_,
to build a cluster on `Amazon Web Services <http://aws.amazon.com/>`_ with an
optional Lustre shared filesystem.

AWS setup
=========

The first time running bcbio on AWS you'll need to setup permissions, VPCs and
local configuration files. We provide commands to automate all these steps and once
finished, they can be re-used for subsequent runs. To start you'll need to have
an account at Amazon and your Access Key ID and Secret Key ID from the
`AWS security credentials page
<https://console.aws.amazon.com/iam/home?#security_credential>`_. These can be
`IAM credentials <https://aws.amazon.com/iam/getting-started/>`_ instead of root
credentials as long as they have administrator privileges. Make them available
to bcbio using the standard environmental variables::

  export AWS_ACCESS_KEY_ID=your_access_key
  export AWS_SECRET_ACCESS_KEY=your_secret_key

With this in place, two commands setup your elasticluster and AWS environment to
run a bcbio cluster. The first creates public/private keys, a bcbio IAM user,
and sets up your elasticluster config in ``~/.bcbio/elasticluster/config``::

  bcbio_vm.py aws iam

The second configures a VPC to host bcbio::

  bcbio_vm.py aws vpc

Running a cluster
=================

Following this setup, you're ready to run a bcbio cluster on AWS.  By default,
the cluster uses the latest pre-built AMI (ami-106aef78, 2014-10-20) with bcbio,
docker and human GRCh37 indices pre-installed.  It will start up one m3.large
head node and two m3.large worker nodes. You can adjust the number of nodes and
sizes by editing your ``~/.bcbio/elasticluster/config``.  Start the cluster
with::

    bcbio_vm.py elasticluster start bcbio

The cluster will take five to ten minutes to start. Once running,
update bcbio wrapper code and Dockerized tools with::

    bcbio_vm.py aws bcbio bootstrap

Finally, connect to the head node with::

    bcbio_vm.py elasticluster ssh bcbio

and run bcbio_vm.py as described in the previous section.

Running Lustre
==============

You can use `Intel Cloud Edition for Lustre (ICEL) <https://wiki.hpdd.intel.com/display/PUB/Intel+Cloud+Edition+for+Lustre*+Software>`_
to set up a Lustre scratch filesystem on AWS.

- Subscribe to `ICEL in the Amazon Marketplace
  <https://aws.amazon.com/marketplace/pp/B00GK6D19A>`_.

- By default, the Lustre filesystem will be 2TB and will be accessible to
  all hosts in the VPC. Creation takes about ten minutes and can happen in
  parallel while elasticluster sets up the cluster. Start the stack::

    bcbio_vm.py aws icel create

- Once the ICEL stack and elasticluster cluster are both running, mount the
  filesystem on the cluster::

    bcbio_vm.py aws icel mount

- The cluster instances will reboot with the Lustre filesystem mounted.

Shutting down
=============

The bcbio Elasticluster and Lustre integration can spin up a lot of AWS
resources. You'll be paying for these by the hour so you want to clean them up
when you finish running your analysis. To stop the cluster::

    bcbio_vm.py elasticluster stop bcbio

To remove the Lustre stack::

    bcbio_vm.py aws icel stop

Double check that all instances have been properly stopped by looking in the AWS
console.

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

Extra software
--------------

We're not able to automatically install some useful tools in pre-built
docker containers due to licensing restrictions. Variant calling with GATK
requires a manual download from the `GATK download`_ site for academic users.
Appistry provides `a distribution of GATK for commercial users`_. Commercial
users also need a license for somatic calling with muTect. To make these jars
available during docker runs, upload them to an S3 bucket you own, and specify
the path to the jars in a global ``resources`` specification in your input sample
YAML file::

    resources:
      gatk:
        jar: s3://bcbio-syn3-eval/jars/GenomeAnalysisTK.jar

If you store your configuration files on S3, bcbio will look for a ``jars``
directory next to your YAML and automatically include the correct
GATK and muTect directives.

.. _GATK download: http://www.broadinstitute.org/gatk/download
.. _a distribution of GATK for commercial users: http://www.appistry.com/gatk

Development Notes
-----------------

These notes are for building containers from scratch or developing on
bcbio-nextgen.

Creating docker image
=====================

An `ansible <http://www.ansible.com>`_ playbook automates the process of
creating the bcbio-nextgen docker images. To build on AWS and upload the latest
image to S3::

    cd ansible
    vim defaults.yml
    ansible-playbook bcbio_vm_docker_aws.yml --extra-vars "@defaults.yml"

or locally, with Docker pre-installed::

    ansible-playbook -c local bcbio_vm_docker_local.yml --extra-vars "@defaults.yml"

Docker image installation
=========================

Install the current bcbio docker image into your local repository by hand with::

    docker import https://s3.amazonaws.com/bcbio_nextgen/bcbio-nextgen-docker-image.gz chapmanb/bcbio-nextgen-devel

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

Amazon Web Services
===================

An ansible script automates preparation of AMIs::

    cd ansible
    vim defaults.yml
    ansible-playbook bcbio_vm_aws.yml --extra-vars "@defaults.yml"

This script doesn't yet terminate EC2 instances, so please manually ensure
instances get cleaned up when developing with it.
