"""Tests for Conway governance DRep functionality."""

import dataclasses
from itertools import chain
import json
import logging
import pathlib as pl
import pickle
import shutil
import typing as tp

import allure
import pytest
from _pytest.fixtures import FixtureRequest
from cardano_clusterlib import clusterlib

from cardano_node_tests.cluster_management import cluster_management
from cardano_node_tests.tests import common
from cardano_node_tests.tests import delegation
from cardano_node_tests.tests import issues
from cardano_node_tests.tests import reqs_conway as reqc
from cardano_node_tests.tests.tests_conway import conway_common
from cardano_node_tests.utils import blockers
from cardano_node_tests.utils import cluster_nodes
from cardano_node_tests.utils import clusterlib_utils
from cardano_node_tests.utils import dbsync_utils
from cardano_node_tests.utils import governance_setup
from cardano_node_tests.utils import governance_utils
from cardano_node_tests.utils import helpers
from cardano_node_tests.utils import submit_utils
from cardano_node_tests.utils.versions import VERSIONS
from cardano_node_tests.utils import submit_api

def get_payment_addr(
    name_template: str,
    cluster_manager: cluster_management.ClusterManager,
    cluster_obj: clusterlib.ClusterLib,
    caching_key: str,
) -> clusterlib.AddressRecord:
    """Create new payment address."""
    with cluster_manager.cache_fixture(key=caching_key) as fixture_cache:
        if fixture_cache.value:
            return fixture_cache.value  # type: ignore

        addr = clusterlib_utils.create_payment_addr_records(
            f"drep_addr_{name_template}",
            cluster_obj=cluster_obj,
        )[0]
        fixture_cache.value = addr

    # Fund source address
    clusterlib_utils.fund_from_faucet(
        addr,
        cluster_obj=cluster_obj,
        faucet_data=cluster_manager.cache.addrs_data["user1"],
        amount=1000000000000
    )

    return addr

@pytest.fixture
def payment_addr(
    cluster_manager: cluster_management.ClusterManager,
    cluster: clusterlib.ClusterLib,
) -> clusterlib.AddressRecord:
    test_id = common.get_test_id(cluster)
    key = helpers.get_current_line_str()
    return get_payment_addr(
        name_template=test_id, cluster_manager=cluster_manager, cluster_obj=cluster, caching_key=key
    )

def get_pool_user(
    name_template: str,
    cluster_manager: cluster_management.ClusterManager,
    cluster_obj: clusterlib.ClusterLib,
    caching_key: str,
    no_of_users: int,
    chunk_size: int = 100,
    fund_amount: int = 1_500_000
) -> tp.List[clusterlib.PoolUser]:
    """Create and fund pool users in chunks."""
    with cluster_manager.cache_fixture(key=caching_key) as fixture_cache:
        if fixture_cache.value:
            return fixture_cache.value  # type: ignore

        pool_users = clusterlib_utils.create_pool_users(
            cluster_obj=cluster_obj,
            name_template=f"{name_template}_pool_user",
            no_of_addr=no_of_users,
        )
        fixture_cache.value = pool_users

    # Fund the payment address with some ADA in chunks
    def fund_pool_users_chunk(pool_users_chunk):
        clusterlib_utils.fund_from_faucet(
            *pool_users_chunk,
            cluster_obj=cluster_obj,
            faucet_data=cluster_manager.cache.addrs_data["user1"],
            amount=fund_amount,
        )

    for i in range(0, len(pool_users), chunk_size):
        chunk = pool_users[i:i + chunk_size]
        fund_pool_users_chunk(chunk)

    return pool_users

@pytest.fixture
def pool_users(
    cluster_manager: cluster_management.ClusterManager,
    cluster: clusterlib.ClusterLib,
) -> tp.List[clusterlib.PoolUser]:
    test_id = common.get_test_id(cluster)
    key = helpers.get_current_line_str()
    return get_pool_user(
        name_template=test_id, cluster_manager=cluster_manager, cluster_obj=cluster, caching_key=key, no_of_users=10000
    )

# register DReps

class TestMassAbstain:
    """Tests for always abstain votes delegation to DReps."""
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.dbsync
    @pytest.mark.testnets
    @pytest.mark.smoke
    def test_register_drep(
        self,
        cluster: clusterlib.ClusterLib,
        payment_addr: clusterlib.AddressRecord,
    ):
        """Test DRep registration.

        * register 10 DRep
        * check that DReps were registered
        """
        # pylint: disable=too-many-locals
        temp_template = common.get_test_id(cluster)
        errors_final = []

        # Register DRep
        num_dreps = 10
        drep_metadata_urls = [f"https://www.the-drep-{i}.com" for i in range(num_dreps)]
        drep_metadata_files = [f"{temp_template}_drep_metadata_{i}.json" for i in range(num_dreps)]
        drep_metadata_contents = [{"name": "The DRep", "ranking": f"{i}"} for i in range(num_dreps)]
        for i in range(num_dreps):
            helpers.write_json(out_file=drep_metadata_files[i], content=drep_metadata_contents[i])
        reqc.cli012.start(url=helpers.get_vcs_link())
        drep_metadata_hashes = [
            cluster.g_conway_governance.drep.get_metadata_hash(
                drep_metadata_file=drep_metadata_files[i]
            )
            for i in range(num_dreps)
        ]

        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli008, reqc.cli009, reqc.cli010, reqc.cip021)]
        
        # Get the current script's directory
        script_dir = pl.Path(__file__).parent
        # Define the relative path for drep_certs
        drep_certs = script_dir / "../drep_certs"
        
        # Check if the directory exists and create it if it doesn't
        if not drep_certs.exists():
            drep_certs.mkdir(parents=True, exist_ok=True)
        
        # Remove all contents of the directory
        if drep_certs.exists() and drep_certs.is_dir():
            shutil.rmtree(drep_certs)
            drep_certs.mkdir(parents=True, exist_ok=True)
            
        reg_dreps = [
            governance_utils.get_drep_reg_record(
                cluster_obj=cluster,
                name_template=f"{temp_template}_{i}",
                drep_metadata_url=drep_metadata_urls[i],
                drep_metadata_hash=drep_metadata_hashes[i],
                destination_dir=drep_certs
            )
            for i in range(num_dreps)
        ]
        [r.success() for r in (reqc.cli008, reqc.cli009, reqc.cli010, reqc.cip021)]

        total_deposit = reg_dreps[0].deposit * num_dreps
        
        drep_registration_certificates = [reg_drep.registration_cert for reg_drep in reg_dreps]
        drep_skey_files = [reg_drep.key_pair.skey_file for reg_drep in reg_dreps]
        
        def check_balance(tx_output_reg):
            reg_out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output_reg)
            assert (
                clusterlib.filter_utxos(utxos=reg_out_utxos, address=payment_addr.address)[0].amount
                == clusterlib.calculate_utxos_balance(tx_output_reg.txins)
                - tx_output_reg.fee
                - (total_deposit)
            ), f"Incorrect balance for source address `{payment_addr.address}`"
        
        def submit_tx(tx_files):
            return clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_reg",
                src_address=payment_addr.address,
                tx_files=tx_files,
                deposit=total_deposit,
            )
        
        tx_files_reg = clusterlib.TxFiles(
            certificate_files=drep_registration_certificates,
            signing_key_files=[payment_addr.skey_file] + drep_skey_files,
        )
        tx_output_reg = submit_tx(tx_files_reg)
        check_balance(tx_output_reg)

        reqc.cli033.start(url=helpers.get_vcs_link())
        for reg_drep in reg_dreps:
            reg_drep_state = cluster.g_conway_governance.query.drep_state(
                drep_vkey_file=reg_drep.key_pair.vkey_file
            )
            assert reg_drep_state[0][0]["keyHash"] == reg_drep.drep_id, "DRep was not registered"
            reqc.cli033.success()
            try:
                dbsync_utils.check_drep_registration(drep=reg_drep, drep_state=reg_drep_state)
            except AssertionError as exc:
                str_exc = str(exc)
                errors_final.append(f"DB-Sync unexpected DRep registration error: {str_exc}")
            reqc.cli012.success()
    
    
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.testnets
    @pytest.mark.smoke
    @pytest.mark.load_test
    def test_dreps_abstain_delegation(
        self,
        cluster: clusterlib.ClusterLib,
        payment_addr: clusterlib.AddressRecord,
        pool_users: clusterlib.PoolUser,
    ):
        """Test mass abstain delegating to DReps.

        * register 9990 stake addresses that will delegate to abstain
        * register 10 stake addresses that will delegate to the 10 DReps
        * check that the stake addresses are registered
        * check that stake addresses delegated to the correct DReps
        """
        
        def chunkify(lst, n):
            return [lst[i:i + n] for i in range(0, len(lst), n)]
        
        # pylint: disable=too-many-statements,too-many-locals
        temp_template = common.get_test_id(cluster)
        deposit_amt = cluster.g_query.get_address_deposit()

        # Create stake address registration cert
        reqc.cli027.start(url=helpers.get_vcs_link())
        reqc.cli027.success()
        
        # select delegatees and delegate to 10 dreps
        
        # Create vote delegation cert
        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli029, reqc.cip022)]
        dreps = json.loads(helpers.run_command("cardano-cli conway query drep-state --all-dreps --testnet-magic 42"))
        drep_key_hashes = [drep[0]["keyHash"] for drep in dreps]
        
        delegatees = pool_users[-10:]
        delegatees_reg_cert = [
            cluster.g_stake_address.gen_stake_addr_registration_cert(
                addr_name=f"{temp_template}_addr_stake_delegatee_{i}",
                deposit_amt=deposit_amt,
                stake_vkey_file=delegatees[i].stake.vkey_file,
            )
            for i in range(len(delegatees))
        ]
        delegatee_deleg_certs = [
            cluster.g_stake_address.gen_vote_delegation_cert(
                addr_name=f"{temp_template}_addr_drep_delegatee_{i}",  # Use a unique address name
                stake_vkey_file=delegatees[i].stake.vkey_file,
                drep_key_hash=drep_key_hashes[i],
                always_abstain=False,
                always_no_confidence=False,
            )
            for i in range(len(delegatees))
        ]
        delegatee_skey_file = [delegatee.stake.skey_file for delegatee in delegatees]
        delegation_tx_files = clusterlib.TxFiles(
            certificate_files=delegatees_reg_cert + delegatee_deleg_certs,
            signing_key_files=[payment_addr.skey_file]+delegatee_skey_file,
        )
        delegatee_address_utxos = [cluster.g_query.get_utxo(delegatee.payment.address) for delegatee in delegatees]
        delegatee_flatenned_utxos = list(chain.from_iterable(delegatee_address_utxos))
        delegatee_tx_output =  clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=payment_addr.address,
                tx_files=delegation_tx_files,
                deposit=(deposit_amt*(len(delegatees))),
            )
        
        # remaining ada holders delegate to abstain 
        
        abstainees = pool_users[:-10]
        abstainees_reg_certs = [
            cluster.g_stake_address.gen_stake_addr_registration_cert(
                addr_name=f"{temp_template}_addr_stake_abstainee_{i}",
                deposit_amt=deposit_amt,
                stake_vkey_file=abstainees[i].stake.vkey_file,
            )
            for i in range(len(abstainees))
        ]
        abstainees_deleg_certs = [
            cluster.g_stake_address.gen_vote_delegation_cert(
                addr_name=f"{temp_template}_addr_drep_abstainee_{i}",  # Use a unique address name
                stake_vkey_file=abstainees[i].stake.vkey_file,
                drep_key_hash="",
                always_abstain=True,
                always_no_confidence=False,
            )
            for i in range(len(abstainees))
        ]
        abstainee_stake_skey_files = [abstainee.stake.skey_file for abstainee in abstainees]
        abstainee_payment_skey_files = [abstainee.payment.skey_file for abstainee in abstainees]
        def submit_tx(abstainees, tx_files): 
            inputs = get_inputs(abstainees)
            num_deposits = len(abstainees)
            return clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=payment_addr.address,
                tx_files=tx_files,
                deposit=(deposit_amt*num_deposits),
            )
        
        def get_inputs(users):
            address_utxos = [cluster.g_query.get_utxo(user.payment.address) for user in users]
            return list(chain.from_iterable(address_utxos))
        
        abstainees_reg_cert_chunks = chunkify(abstainees_reg_certs, 30)
        abstainees_deleg_cert_chunks = chunkify(abstainees_deleg_certs, 30)
        abstainee_stake_skey_file_chunks = chunkify(abstainee_stake_skey_files, 30)
        abstainee_payment_skey_file_chunks = chunkify(abstainee_payment_skey_files, 30)
        abstainee_chunks = chunkify(abstainees, 30)
        
        zipped_chunks = list(zip(
                abstainees_reg_cert_chunks,
                abstainees_deleg_cert_chunks,
                abstainee_stake_skey_file_chunks,
                abstainee_payment_skey_file_chunks,
                abstainee_chunks
        ))
        
        abstainee_tx_outputs = []
        
        for abstainee_info in zipped_chunks:
            chunked_tx_files = clusterlib.TxFiles(
                certificate_files= abstainee_info[0] + abstainee_info[1],
                signing_key_files= [payment_addr.skey_file] + abstainee_info[2] + abstainee_info[3]     
            )
            abstainee_tx_outputs.append(submit_tx(abstainee_info[4], chunked_tx_files))

        # check correcct delegations
        
        # check delegatees
        for i in range(len(delegatees)): 
            stake_address_info = cluster.g_query.get_stake_addr_info(delegatees[i].stake.address)
            assert (
                stake_address_info.address
            ), f"Stake address is NOT registered: {delegatees[i].stake.address}"
            assert stake_address_info.vote_delegation == governance_utils.get_drep_cred_name(
                drep_id=drep_key_hashes[i]
            ), "Votes are NOT delegated to the correct DRep"
        
        # check abstainees 
        for i in range(len(abstainees)): 
            stake_address_info = cluster.g_query.get_stake_addr_info(abstainees[i].stake.address)
            assert (
                stake_address_info.address
            ), f"Stake address is NOT registered: {abstainees[i].stake.address}"
            assert stake_address_info.vote_delegation == governance_utils.get_drep_cred_name(
                drep_id="always_abstain"
            ), "Votes are NOT delegated to the correct DRep"
    
    ## drep votes for committee threshold update 
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.testnets
    @pytest.mark.load_test
    def test_dreps_vote_committee_update(self,
        pool_users: clusterlib.PoolUser,
        cluster: clusterlib.ClusterLib,
        payment_addr: clusterlib.AddressRecord,
        ):
        """Test delegated DReps vote on an update committee action.

        * create an udpate committee threshold action
        * 10 registered DReps and 3 SPOs cast vote
        * check that the action is ratified
        """
        delegatees = pool_users[-10:]
        pool_user = pool_users[0]
        
        pool_user_amount = cluster.g_query.get_address_balance(pool_user.payment.address)
        assert pool_user_amount, "Pool user has 0 balance"
        
        # check delegatees have balance 
        for delegatee in delegatees: 
            amount = cluster.g_query.get_address_balance(delegatee.payment.address)
            assert amount, "Delegatee has 0 balance"

        # Get the current script's directory
        script_dir = pl.Path(__file__).parent.parent.parent.parent
                
        temp_template = common.get_test_id(cluster)
        deposit_amt = cluster.conway_genesis["govActionDeposit"]
        prev_action_rec = governance_utils.get_prev_action(
            action_type=governance_utils.PrevGovActionIds.COMMITTEE,
            gov_state=cluster.g_conway_governance.query.gov_state(),
        )
        cc_size = 7
        threshold = "2/3"
        
        reqc.cip031a_01.start(url=helpers.get_vcs_link())
        update_action = cluster.g_conway_governance.action.update_committee(
                action_name=f"{temp_template}",
                deposit_amt=deposit_amt,
                anchor_url=f"http://www.cc-threshold-update.com",
                anchor_data_hash="5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d",
                threshold=threshold,
                prev_action_txid=prev_action_rec.txid,
                prev_action_ix=prev_action_rec.ix,
                deposit_return_stake_vkey_file=pool_user.stake.vkey_file,
            )
        reqc.cip031a_01.success()
        tx_file = clusterlib.TxFiles(
            proposal_files = [update_action.action_file],
            signing_key_files  = 
                [pool_user.payment.skey_file, payment_addr.skey_file] 
        )
        if conway_common.is_in_bootstrap(cluster_obj=cluster):
            with pytest.raises((clusterlib.CLIError, submit_api.SubmitApiError)) as excinfo:
                clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_bootstrap",
                    src_address=payment_addr.address,
                    tx_files=tx_file,
                    deposit=deposit_amt,
                )
            err_str = str(excinfo.value)
            assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
            return
        reqc.cip007.start(url=helpers.get_vcs_link())
        tx_output = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=payment_addr.address,
                tx_files=tx_file,
                deposit=deposit_amt,
            )
        out_utxo = cluster.g_query.get_utxo(tx_raw_output=tx_output)
        assert (
                clusterlib.filter_utxos(utxos=out_utxo, address=payment_addr.address)[0].amount
                == clusterlib.calculate_utxos_balance(tx_output.txins) - tx_output.fee - deposit_amt
            ), f"Incorrect balance for source address `{pool_user.payment.address}`"
        action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output.out_file)
        gov_state = cluster.g_conway_governance.query.gov_state()
        prop = governance_utils.lookup_proposal(gov_state=gov_state, action_txid=action_txid)
        assert prop, "Update committee action not found"
        assert (
                    prop["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.UPDATE_COMMITTEE.value
                ), "Incorrect action tag"
        
        # dreps cast vote

        # Define the relative path for drep_certs
        drep_cred_path = script_dir / "cardano_node_tests/tests/drep_certs"
        drep_vkey_files = list(drep_cred_path.glob("*.vkey"))
        drep_skey_files = list(drep_cred_path.glob("*.skey"))
        dreps = json.loads(helpers.run_command("cardano-cli conway query drep-state --all-dreps --testnet-magic 42"))
        drep_votes = []
        for i in range(len(dreps)):
            drep_votes.append(cluster.g_conway_governance.vote.create_drep(
                vote_name = f"{temp_template}_{action_txid}_drep_vote_{i}",
                action_txid =  action_txid,
                action_ix = 0,
                vote = clusterlib.Votes.YES,
                drep_vkey_file = drep_vkey_files[i] 
            ))
        conway_common.submit_vote_(
            cluster_obj = cluster, 
            name_template=f"{temp_template}_dreps_vote",
            payment_addr=payment_addr,
            votes=drep_votes,
            keys=drep_skey_files
        )
        
        # pool cast vote 
        pool1 = script_dir / "dev_workdir/state-cluster0/nodes/node-pool1"
        pool2 = script_dir / "dev_workdir/state-cluster0/nodes/node-pool2"
        pool3 = script_dir / "dev_workdir/state-cluster0/nodes/node-pool3"
        
        # Function to get cold.vkey and cold.skey files
        def get_cold_keys(pool_path):
            cold_vkey = pool_path / "cold.vkey"
            cold_skey = pool_path / "cold.skey"
            if cold_vkey.exists() and cold_skey.exists():
                return (cold_vkey, cold_skey)
            else:
                return None
        
        cold_keys = [get_cold_keys(pool1), get_cold_keys(pool2), get_cold_keys(pool3)]
        pool_skeys = [cold_key[1] for cold_key in cold_keys]
        pool_vkeys = [cold_key[0] for cold_key in cold_keys]
        
        pool_votes = []
        for i in range(3):
            pool_votes.append(
                cluster.g_conway_governance.vote.create_spo(
                    vote_name=f"{temp_template}_{action_txid}_vote_spo_{i}",
                    action_txid =  action_txid,
                    action_ix = 0,
                    vote = clusterlib.Votes.YES,
                    cold_vkey_file=pool_vkeys[i]
                )
            )
        
        conway_common.submit_vote_(
            cluster_obj = cluster, 
            name_template=f"{temp_template}_spo_vote",
            payment_addr=payment_addr,
            votes=pool_votes,
            keys=pool_skeys
        )
        
        # check expired or enacted 
        while True:
            gov_state = cluster.g_conway_governance.query.gov_state()
            # check if in proposal
            prop_action = governance_utils.lookup_proposal(gov_state=gov_state, action_txid=action_txid)
            rat_action = governance_utils.lookup_ratified_actions(gov_state=gov_state, action_txid=action_txid)
            expired_action = governance_utils.lookup_expired_actions(gov_state=gov_state, action_txid=action_txid)
            if ((not prop_action) and (not rat_action) and (not expired_action)):
                break
            if  (rat_action or expired_action): 
                break

        assert rat_action, f"Action {action_txid} not ratified"