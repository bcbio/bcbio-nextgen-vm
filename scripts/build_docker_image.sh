#!/bin/bash
set -eu -o pipefail
# setup for local ssh
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ''
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh-keyscan -t rsa localhost >> ~/.ssh/known_hosts
# change docker mount location to larger /encrypted partition
sudo service docker stop
sudo mv /var/lib/docker /encrypted/docker
sudo mkdir -p /encrypted/docker-tmp
sudo sh -c "echo 'DOCKER_OPTS=\"-g /encrypted/docker\"' >> /etc/default/docker"
sudo sh -c "echo 'export TMPDIR=\"/encrypted/docker-tmp\"' >> /etc/default/docker"
sudo service docker start
# Run docker build
bcbio_vm.py devel dockerbuild
