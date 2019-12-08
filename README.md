# Distributed Database

using py3 enviroment
pre-install Vagrant and Orable VirtualBox on your workstation


# Usage
## Run experiment locally
We use python3 environment.
required python module and tools:
```bash
# Build python3 environment
conda create -name py3 python=3
source activate py3

# Load required modules
pip install absl-py
```
run experiment:
```bash
python parser.py --filename=test.txt
```

## Run experiment in VM and generate reproducible experiment package.
required tools:
Vagrant
Oracle VirtualBox

in locol terminal, run:
```bash
git clone https://github.com/Krystal-158/DistributedDatabase.git
cd DistributedDatabase

# Build and enter virtual machine
vagrant up
vagrant ssh
cd /vagrant/code
ls

# run experiment
python3 parser.py --filename=test/test1.txt

# trace experiment with reprozip
reprozip trace python3 parser.py --filename=test/test1.txt
reprozip trace --continue python3 parser.py --filename=<other test>

# pack the experiment
reprozip pack advDB

# shut down virtual machine
exit
vagrant halt

# delete virtual machine
vagrant destroy
```

This will generate a reprozip package called advDB.rpz in current directory.

## Reproduce the experiment with advDB.rpz

[more command for reproduce experiment](https://docs.reprozip.org/en/1.0.x/unpacking.html#reproducing-the-experiment)

In your local terminal, find this advDB.rpz and run:

```bash
# install reprounzip
pip install reprounzip-vagrant

# goes to the directory of advDB.rpz and look up info
cd <advDB path>
reprounzip info advDB.rpz

# unpack it into a VM where you reproduce the experiment
reprounzip vagrant setup advDB.rpz <path>
reprounzip vagrant run <path>
reprounzip vagrant run <path> <run-id>
# reproduce all the running with id from 0 to 10
reprounzip vagrant run <path> 0-10

# end experiment and delete unpacked files
reprounzip vagrant destroy <path>
```






