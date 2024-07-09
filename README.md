# Governance Action Load Testing (based on cardano-node-tests)
>This is an implementation of cardano-node-tests for governance action load testing 

### Agents 
- 3 SPOs 
- 90 Committee Members
- 100 DReps

### Load Tests Applied on Following Gov Actions 
- Drep and SPO Delegation
- Committee Update
- Constitution Update
- Protocol Patameter Update
- Treasury Withdrawal 
- HardFork 
- Info
- No Confidence 

### Load Test Scenarios

#### Scenario 1: (majority)  (repeat for: each governance action type)
- Create 3  proposal of this type
- Each stake holders votes on the proposal with "proposal1" having the majority
- Proposal1 is enacted. 

#### Scenario 2: (equal)  (repeat for: each governance action type)
- Create 3  proposal of this type
- Each stake holders votes on the proposals with "proposal1" and "proposal2" having equal votes. 
- The proposal submitted first to the chain is enacted

#### Scenario 3: (insufficient) (repeat for: each governance action type) 
- Create 3  proposal of this type
- Each stake holders votes on the proposals, but none reaching the threshold
- None of the proposals are enacted


### To run tests individually

1. run nix shell that has all the needed dependencies

    ```sh
    nix flake update --accept-flake-config --override-input cardano-node "github:IntersectMBO/cardano-node/master"  # change `master` to rev you want
    nix develop --accept-flake-config .#venv
    ```

2. prepare testing environment

    ```sh
    source ./prepare_test_env.sh conway
    ```

3. start the cluster instance

    > Testing HardFork requires major protocol version 9
    ```sh
    ./dev_workdir/babbage_fast/start-cluster
    ```
    > Testing other gov actions require major protocol version 10
    ```sh
    PV10=true ./dev_workdir/babbage_fast/start-cluster
    ```

4. run some tests 
    > To run a governance action load test with all scenarios
    ```sh
    pytest cardano_node_tests/tests/tests_conway/test_hardfork.py
    ```
    > To run governance action load test with a specific scenario
    ```sh
    pytest -s -k test_hardfork_majority cardano_node_tests/tests/tests_conway/test_hardfork.py
    ```

5. stop the cluster instance

    ```sh
    ./dev_workdir/babbage_fast/stop-cluster
    ```
### To view test results
    ```sh
    ./cardano_node_tests/tests/tests_conway/allure_report.sh
    ```