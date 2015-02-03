#!/bin/bash
set -e
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ''
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh-keyscan -t rsa localhost >> ~/.ssh/known_hosts
bcbio_vm.py devel dockerbuild
