# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|

  config.vm.box = "bento/ubuntu-18.04"

  config.vm.hostname = "bcbio-vm"

  config.vm.provider :virtualbox do |vb|
    vb.memory = 4096
    vb.cpus = 2
  end

  # to make any additional data on the host available inside the VM
  # (for example: reference genomes, pipeline inputs, etc)
  # set BCBIO_DATA_DIR environment variable on the host to a directory that contains the data
  if ENV["BCBIO_DATA_DIR"]
    config.vm.synced_folder ENV["BCBIO_DATA_DIR"], "/data"
  end

  config.vm.provision :shell, :path => "vagrant.sh"

end
