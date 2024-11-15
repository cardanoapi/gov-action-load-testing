# Ensure the script is called with an argument
if [ -z "$1" ]; then
  echo "Usage: $0 <wallet_address>"
  exit 1
fi

# Assign the argument to WALLET_ADDRESS
WALLET_ADDRESS=$1

export SOCKET_PATH='./dev_workdir/state-cluster0/bft1.socket'
export GENESIS_ADDRESS=$(cat ./dev_workdir/state-cluster0/shelley/genesis-utxo.addr)
export GENESIS_SKEY_FILE=./dev_workdir/state-cluster0/shelley/genesis-utxo.skey
cardano-cli query utxo --address $GENESIS_ADDRESS --testnet-magic 42 --out-file ./chain-state/genesis-txin.json
export GENESIS_TXIN=$(jq -r 'keys[0]' ./chain-state/genesis-txin.json)

cardano-cli conway transaction build \
    --tx-in $GENESIS_TXIN \
    --tx-out $WALLET_ADDRESS+29000000000000000 \
    --out-file .cluster-address/fund-wallet-address.tx \
    --change-address $GENESIS_ADDRESS \
    --testnet-magic 42 \
    --socket-path $SOCKET_PATH

cardano-cli conway transaction sign \
    --tx-body-file .cluster-address/fund-wallet-address.tx \
    --signing-key-file $GENESIS_SKEY_FILE \
    --testnet-magic 42 \
    --out-file .cluster-address/fund-wallet-address.tx \

cardano-cli conway transaction submit \
    --tx-file .cluster-address/fund-wallet-address.tx \
    --testnet-magic 42

echo "Funding Wallet..."
sleep 5

echo "WALLET BALANCE:"
cardano-cli query utxo --address $WALLET_ADDRESS --testnet-magic 42  --socket-path $SOCKET_PATH