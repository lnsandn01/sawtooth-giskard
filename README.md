# Practical analysis of the Giskard Consensus Protocol

This repository is an implementation of the Giskard Consensus Protocol.
The Giskard consensus protocol is used to validate transactions and computations in the [PlatON network](https://platon.network) and tolerates Byzantine failures among participating nodes.
This implementation integrates the protocol into the blockchain-framework Hyperledger Sawtooth, for testing its safety and liveness property.

## Quicklinks
- In earlier work from 2020, Giskard was specified [informally](https://arxiv.org/abs/2010.02124) 
- as well as [formally in the Coq proof assistant](https://github.com/runtimeverification/giskard-verification).
- The Coq formalization is described in a [technical report](https://github.com/runtimeverification/giskard-verification/releases/download/v1.0/report.pdf).
- [Hyperledger Sawtooth framework](https://www.hyperledger.org/use/sawtooth)

# Installation
!Warning! The project is still in a cleanup phase, installation might not work yet without problems, and testing is still partially manually instrumented!

## System Requirements
This library is developed on and intended for systems running:
- Ubuntu 18.04 (Bionic) [installation guide](https://ubuntu.com/tutorials/install-ubuntu-desktop-1804#1-overview)
- Python version 3.6.9 [installation guide](https://linuxhint.com/install-specific-python-version-ubuntu/)

If you do not want to install Ubuntu onto your computer, consider installing a
[virtual machine](https://www.osboxes.org/ubuntu/)

## Installation Steps
1. Install [Sawtooth Hyperledger](https://sawtooth.hyperledger.org/docs/1.2/sysadmin_guide/setting_up_sawtooth_network.html) and choose the steps with PoET as the consensus protocol
2. Download this repository, and replace the files in the respective packages of sawtooth, in the folder /usr/lib/python3/dist-packages/ with the files from this repo
  - folder in question "sawtooth_poet_engine", "sawtooth_poet_tests"
3. Install python dependencies by calling: pip install -r requirements.txt

## Running the Tests
Go to the folder /usr/lib/python3/dist-packages/ and run the desired tests with the command:

```
sudo nose2-3 -c sawtooth_poet_tests/nose2.cfg -v -s sawtooth_poet_tests/ test_giskard_network.TestGiskardNetwork.test_giskard_network > /sawtooth_poet_tests/text.txt  2>&1
```

This will store the logs from all nodes and the test into a text file. Replace *test_giskard_network.TestGiskardNetwork.**test_giskard_network*** with the desired test. You can find the tests in the file test_giskard_network.py in the folder tests/sawtooth_poet_tests/
