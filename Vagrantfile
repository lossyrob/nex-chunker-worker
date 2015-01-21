# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.box_check_update = false

  config.vm.define "worker" do |worker|
    worker.vm.network :private_network, ip: "192.168.88.88"
    worker.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "3048"]
    end

    worker.vm.synced_folder ".", "/vagrant", disabled: true
    worker.vm.synced_folder "src/chunker", "/opt/chunker/"

    worker.vm.provision :ansible do |ansible|
      ansible.playbook = "deploy/run-chunker.yml"
      ansible.groups = { "worker" => ["worker"], "development:children" => ["worker"] }
    end
  end
end
