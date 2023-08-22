# Practical analysis of the Giskard Consensus Protocol

This repository is an implementation of the Giskard Consensus Protocol.
The Giskard consensus protocol is used to validate transactions and computations in the [PlatON network](https://platon.network) and tolerates Byzantine failures among participating nodes.
This implementation integrates the protocol into the blockchain-framework Hyperledger Sawtooth, for testing its safety and liveness property.

## Quicklinks
- In earlier work from 2020, Giskard was specified [informally](https://arxiv.org/abs/2010.02124) 
- as well as [formally in the Coq proof assistant](https://github.com/runtimeverification/giskard-verification).
- The Coq formalization is described in a [technical report](https://github.com/runtimeverification/giskard-verification/releases/download/v1.0/report.pdf).
- [Hyperledger Sawtooth framework](https://www.hyperledger.org/use/sawtooth)
- Previously recorded [test data](https://github.com/lnsandn01/sawtooth-giskard/tree/1b694cd9829875cf26b544df46b1466aef5787b9/tests/sawtooth_poet_tests/test_logs) for comparison

# Installation

## System Requirements
This library is developed on and intended for systems running:
- Ubuntu 18.04 (Bionic) [installation guide](https://ubuntu.com/tutorials/install-ubuntu-desktop-1804#1-overview)
- Python version 3.6.9 [installation guide](https://linuxhint.com/install-specific-python-version-ubuntu/)

If you do not want to install Ubuntu onto your computer, consider installing a
[virtual machine](https://www.osboxes.org/ubuntu/)

## Installation Steps
1. Install Sawtooth Hyperledger with the poet consensus engine
  - ```sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 8AA7AF1F1091A5FD```
  - ```sudo add-apt-repository 'deb [arch=amd64] http://repo.sawtooth.me/ubuntu/chime/stable bionic universe'```

  - ```sudo apt-get update```

  - ```sudo apt-get install -y sawtooth python3-sawtooth-poet-cli python3-sawtooth-poet-engine python3-sawtooth-poet-families```

2. Download this repository to /home/repos/
   - ```cd /home```
   - ```mkdir repos```
   - ```cd repos```
   - ```git clone git@github.com:lnsandn01/sawtooth-giskard.git```

4. Install tools and dependencies
  - ```sudo apt-get install -y python3-pip```
  - ```sudo apt-get install -y python3-nose2```

  - ```sudo cp -a /mnt/c/repos/sawtooth-giskard/common/requirements.txt  /usr/lib/python3/dist-packages    /sawtooth_poet_common/```

  - ```cd /usr/lib/python3/dist-packages/sawtooth_poet_common/```
  - ```pip3 install -r requirements.txt```
    
4. Replace the files in your local sawtooth install with my code
``` cd /usr/lib/python3/dist-packages/sawtoot_poet_simulator
sudo mkdir packaging

sudo cp -a sawtooth-giskard/engine/sawtooth_poet_engine/{engine.py,giskard.py,giskard_block.py,main.py,giskard_global_state.py,giskard_global_trace.py,giskard_message.py,giskard_node.py,giskard_nstate.py,giskard_state_transition_type.py,oracle.py} /usr/lib/python3/dist-packages/sawtooth_poet_engine/

sudo cp -a sawtooth-giskard/tests/sawtooth_poet_tests/ /usr/lib/python3/dist-packages/

sudo cp -a sawtooth-giskard/core/sawtooth_poet/journal/block_wrapper.py /usr/lib/python3/dist-packages/sawtooth_poet/journal/

sudo cp -a sawtooth-giskard/simulator/packaging/simulator_rk_pub.pem /usr/lib/python3/dist-packages/sawtooth_poet_simulator/packaging/


sudo cp -a sawtooth-giskard/sawtooth-sdk-python/sawtooth_sdk/consensus/ /usr/lib/python3/dist-packages/sawtooth_sdk/

sudo cp -a sawtooth-giskard/sawtooth-sdk-python/sawtooth_sdk/messaging/  /usr/lib/python3/dist-packages/sawtooth_sdk/


sudo cp -a sawtooth-giskard/sawtooth-core/validator/sawtooth_validator/consensus/notifier.py /usr/lib/python3/dist-packages/sawtooth_validator/consensus/

sudo cp -a sawtooth-giskard/sawtooth-core/validator/sawtooth_validator/networking/dispatch.py /usr/lib/python3/dist-packages/sawtooth_validator/networking/
```

## Running the Tests

```
cd /usr/lib/python3/dist-packages/

sudo nose2-3 \
-c sawtooth_poet_tests/nose2.cfg \
-v \
-s \
sawtooth_poet_tests/ \
test_giskard_network.TestGiskardNetwork.test_giskard_network 
```

Edit the test variables in the file */usr/lib/python3/dist-packages/sawtooth_poet_tests/test_giskard_network.py* in the function *test_giskard_network*
to test different behaviour of the Giskard protocol in different situations: *dishonest_nodes*(possible_values:0-4) or *timeout_test*(possible_values:0-4).
<br><br>
Before running the test again, or to stop the test:
```
ps ax | grep 'sawtooth-validator' | awk -F ' ' '{print $1}' | xargs sudo kill -9
sudo pkill poet
sudo pkill intkey
ps ax | grep 'settings-tp' | awk -F ' ' '{print $1}' | xargs sudo kill -9
sudo pkill sawtooth
```
