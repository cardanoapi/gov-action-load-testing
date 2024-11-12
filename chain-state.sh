cardano-cli conway query protocol-parameters --testnet-magic 42 > chain-state/protocol-parameters.json
cardano-cli conway query gov-state --testnet-magic 42 > chain-state/gov-state.json
cardano-cli conway query drep-state --all-dreps --testnet-magic 42 > chain-state/dreps.json
cardano-cli query tip --testnet-magic 42 > chain-state/chain-tip.json