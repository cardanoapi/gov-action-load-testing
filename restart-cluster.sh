./dev_workdir/conway_fast/stop-cluster
rm -rf ./dev_workdir/
source ./prepare_test_env.sh conway
PV10=true ./dev_workdir/conway_fast/start-cluster