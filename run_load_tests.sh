
source ./prepare_test_env.sh conway
./dev_workdir/conway_fast/start-cluster 

## hardfork action
pytest -s -k test_hardfork cardano_node_tests/tests/tests_conway/test_hardfork.py

## update constitution action
pytest -s -k test_change_constitution cardano_node_tests/tests/tests_conway/test_constitution.py

## dRep delegation
pytest -s -k test_dreps_delegation cardano_node_tests/tests/tests_conway/test_drep.py
pytest -s -k test_dreps_and_spo_delegation cardano_node_tests/tests/tests_conway/test_drep.py

## protocol param update action
pytest -s -k test_pparam_update cardano_node_tests/tests/tests_conway/test_pparam_update.py

## info action
pytest -s -k test_info cardano_node_tests/tests/tests_conway/test_info.py 

## update committee action
pytest -s -k test_register_and_resign_committee_member cardano_node_tests/tests/tests_conway/test_committee.py 
pytest -s -k test_update_committee_action cardano_node_tests/tests/tests_conway/test_committee.py 

## treasury withdrawl action
pytest -s -k test_treasury_withdrawl cardano_node_tests/tests/tests_conway/test_treasury_withdrawals.py 

## no confidence action
pytest -s -k test_no_confidence_action cardano_node_tests/tests/tests_conway/test_no_confidence.py

./dev_workdir/conway_fast/stop-cluster