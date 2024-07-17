"""Tests for Conway governance Constitutional Committee functionality."""

# pylint: disable=expression-not-assigned
from itertools import chain
import logging
import pathlib as pl
import typing as tp

import allure
import hypothesis
import hypothesis.strategies as st
import pytest
from _pytest.fixtures import FixtureRequest
from cardano_clusterlib import clusterlib

from cardano_node_tests.cluster_management import cluster_management
from cardano_node_tests.tests import common
from cardano_node_tests.tests import issues
from cardano_node_tests.tests import reqs_conway as reqc
from cardano_node_tests.tests.tests_conway import conway_common
from cardano_node_tests.utils import clusterlib_utils
from cardano_node_tests.utils import configuration
from cardano_node_tests.utils import dbsync_utils
from cardano_node_tests.utils import governance_setup
from cardano_node_tests.utils import governance_utils
from cardano_node_tests.utils import helpers
from cardano_node_tests.utils import submit_api
from cardano_node_tests.utils import submit_utils
from cardano_node_tests.utils.versions import VERSIONS

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(
    VERSIONS.transaction_era < VERSIONS.CONWAY,
    reason="runs only with Tx era >= Conway",
)


@pytest.fixture
def payment_addr_comm(
    cluster_manager: cluster_management.ClusterManager,
    cluster_use_committee: governance_setup.GovClusterT,
) -> clusterlib.AddressRecord:
    """Create new payment address."""
    cluster, __ = cluster_use_committee
    with cluster_manager.cache_fixture() as fixture_cache:
        if fixture_cache.value:
            return fixture_cache.value  # type: ignore

        addr = clusterlib_utils.create_payment_addr_records(
            f"committee_addr_ci{cluster_manager.cluster_instance_num}",
            cluster_obj=cluster,
        )[0]
        fixture_cache.value = addr

    # Fund source address
    clusterlib_utils.fund_from_faucet(
        addr,
        cluster_obj=cluster,
        faucet_data=cluster_manager.cache.addrs_data["user1"],
    )

    return addr


@pytest.fixture
def pool_users(
    cluster_manager: cluster_management.ClusterManager,
    cluster: clusterlib.ClusterLib,
) -> tp.List[clusterlib.PoolUser]:
    """Create pool users."""
    key = helpers.get_current_line_str()
    name_template = common.get_test_id(cluster)
    return conway_common.get_registered_pool_user(
        cluster_manager=cluster_manager,
        name_template=name_template,
        cluster_obj=cluster,
        caching_key=key,
        no_of_users=20
    )

@pytest.fixture
def pool_users_lg(
    cluster_manager: cluster_management.ClusterManager,
    cluster_lock_governance: governance_setup.GovClusterT,
) -> tp.List[clusterlib.PoolUser]:
    """Create a pool user for "lock governance"."""
    cluster, __ = cluster_lock_governance
    key = helpers.get_current_line_str()
    name_template = common.get_test_id(cluster)
    return conway_common.get_registered_pool_user(
        cluster_manager=cluster_manager,
        name_template=name_template,
        cluster_obj=cluster,
        caching_key=key,
        no_of_users=20
    )


class TestCommittee:
    """Tests for Constitutional Committee."""

    @allure.link(helpers.get_vcs_link())
    @common.PARAM_USE_BUILD_CMD
    @pytest.mark.dbsync
    @pytest.mark.testnets
    @pytest.mark.smoke
    @pytest.mark.load_test
    def test_register_and_resign_committee_member(
        self,
        cluster_use_committee: governance_setup.GovClusterT,
        payment_addr_comm: clusterlib.AddressRecord,
        use_build_cmd: bool,
    ):
        """Test Constitutional Committee Member registration and resignation.

        * register a potential CC Member
        * check that CC Member was registered
        * resign from CC Member position
        * check that CC Member resigned
        """
        cc_size = 77
        cluster, __ = cluster_use_committee
        temp_template = common.get_test_id(cluster)
        # Register a potential CC Member
        print(f"\nRegistering {cc_size} Committee members at once.")
        _url = helpers.get_vcs_link()
        [
            r.start(url=_url)
            for r in (reqc.cli003, reqc.cli004, reqc.cli005, reqc.cli006, reqc.cip003)
        ]
        cc_auth_records = [
            governance_utils.get_cc_member_auth_record(
                cluster_obj=cluster,
                name_template=f"{temp_template}_{i}",
            )
            for i in range(1, cc_size + 1)
        ]
        [r.success() for r in (reqc.cli003, reqc.cli004, reqc.cli005, reqc.cli006)]
        auth_certs = [cc_auth_record.auth_cert for cc_auth_record in cc_auth_records]
        cold_skey_files = [cc_auth_record.cold_key_pair.skey_file for cc_auth_record in cc_auth_records]
        tx_files_auth = clusterlib.TxFiles(
            certificate_files=auth_certs,
            signing_key_files=[payment_addr_comm.skey_file]+cold_skey_files,
        )

        try:
            tx_output_auth = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_auth",
                src_address=payment_addr_comm.address,
                use_build_cmd=use_build_cmd,
                tx_files=tx_files_auth,
            )
            reqc.cip003.success()

            auth_out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output_auth)
            assert (
                clusterlib.filter_utxos(utxos=auth_out_utxos, address=payment_addr_comm.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_auth.txins) - tx_output_auth.fee
            ), f"Incorrect balance for source address `{payment_addr_comm.address}`"

            cluster.wait_for_new_block(new_blocks=2)
            _url = helpers.get_vcs_link()
            [r.start(url=_url) for r in (reqc.cli032, reqc.cip002, reqc.cip004)]
            auth_committee_state = cluster.g_conway_governance.query.committee_state()
            member_keys = [f"keyHash-{cc_auth_record.key_hash}" for cc_auth_record in cc_auth_records]
            member_recs = [auth_committee_state["committee"][member_key] for member_key in member_keys ]

            for member_rec in member_recs:
                assert (
                    member_rec["hotCredsAuthStatus"]["tag"] == "MemberAuthorized"
                ), "CC Member was NOT authorized"
                assert not member_rec["expiration"], "CC Member should not be elected"
                assert member_rec["status"] == "Unrecognized", "CC Member should not be recognized"
            [r.success() for r in (reqc.cli032, reqc.cip002, reqc.cip004)]

            # Resignation of CC Member

            _url = helpers.get_vcs_link()
            [r.start(url=_url) for r in (reqc.cli007, reqc.cip012)]
            
            res_certs = [
                cluster.g_conway_governance.committee.gen_cold_key_resignation_cert(
                    key_name=f"{temp_template}_{i}",
                    cold_vkey_file=cc_auth_records[i].cold_key_pair.vkey_file,
                    resignation_metadata_url=f"http://www.cc-resign-{i}.com",
                    resignation_metadata_hash="5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d",
                )
                for i in range(cc_size)
            ]
            reqc.cli007.success()

            tx_files_res = clusterlib.TxFiles(
                certificate_files=res_certs,
                signing_key_files=[payment_addr_comm.skey_file] + cold_skey_files,
            )
            print(f"\nResigning {cc_size} Committee members at once.")
            try:
                tx_output_res = clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_res",
                    src_address=payment_addr_comm.address,
                    use_build_cmd=use_build_cmd,
                    tx_files=tx_files_res,
                )
            except clusterlib.CLIError as exc:
                err_str = str(exc)
                if "MaxTxSizeUTxO" in err_str:
                    print(f"Fails at resigning {cc_size} CC members in a single transaction")
                    return

            cluster.wait_for_new_block(new_blocks=2)
            res_committee_state = cluster.g_conway_governance.query.committee_state()
            res_member_recs = [res_committee_state["committee"].get(member_key) for member_key in member_keys]

            for res_member_rec in res_member_recs:
                assert (
                    not res_member_rec or res_member_rec["hotCredsAuthStatus"]["tag"] == "MemberResigned"
                ), "CC Member not resigned"

            reqc.cip012.success()

            res_out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output_res)
            assert (
                clusterlib.filter_utxos(utxos=res_out_utxos, address=payment_addr_comm.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_res.txins) - tx_output_res.fee
            ), f"Incorrect balance for source address `{payment_addr_comm.address}`"

            # Check CC member in db-sync
            for cc_auth_record in cc_auth_records:
                dbsync_utils.check_committee_member_registration(
                    cc_member_cold_key=cc_auth_record.key_hash, committee_state=auth_committee_state
                )
                dbsync_utils.check_committee_member_deregistration(
                    cc_member_cold_key=cc_auth_record.key_hash
                )        
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at registering {cc_size} CC members in a single transaction")
                return

    @allure.link(helpers.get_vcs_link())
    @common.PARAM_USE_BUILD_CMD
    @pytest.mark.smoke
    @pytest.mark.load_test
    def test_update_committee_action_majority(
        self,
        pool_users_lg: clusterlib.PoolUser,
        cluster_lock_governance: governance_setup.GovClusterT,
        use_build_cmd: bool,
    ):
        """Test update committee action.

        * create 3 proposals to add CC Members with different thresholds 
        * vote majority to approve action 3
        * vote insufficiently to disapprove actions 1 and 2
        * check that action 3 is ratified and enacted 
        """
        num_pool_users = 3
        cc_size = 50
        cluster, governance_data = cluster_lock_governance
        selected_pool_users = pool_users_lg[:num_pool_users]
        temp_template = common.get_test_id(cluster)
        cc_auth_records = [
            governance_utils.get_cc_member_auth_record(
                cluster_obj=cluster,
                name_template=f"{temp_template}_{i}",
            )
            for i in range(1, cc_size + 1)
        ]
        cc_members = [
            clusterlib.CCMember(
                epoch=10_000,
                cold_vkey_file=r.cold_key_pair.vkey_file,
                cold_skey_file=r.cold_key_pair.skey_file,
                hot_vkey_file=r.hot_key_pair.vkey_file,
                hot_skey_file=r.hot_key_pair.skey_file,
            )
            for r in cc_auth_records
        ]

        deposit_amt = cluster.conway_genesis["govActionDeposit"]
        anchor_url = "http://www.cc-update.com"
        anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"
        prev_action_rec = governance_utils.get_prev_action(
            action_type=governance_utils.PrevGovActionIds.COMMITTEE,
            gov_state=cluster.g_conway_governance.query.gov_state(),
        )
        
        thresholds = ["2/3","0/1","1/2"]
        reqc.cip031a_01.start(url=helpers.get_vcs_link())
        update_actions = [
            cluster.g_conway_governance.action.update_committee(
                action_name=f"{temp_template}_{i}",
                deposit_amt=deposit_amt,
                anchor_url=f"http://www.cc-update-{i}.com",
                anchor_data_hash=anchor_data_hash,
                threshold=thresholds[i],
                add_cc_members=cc_members,
                prev_action_txid=prev_action_rec.txid,
                prev_action_ix=prev_action_rec.ix,
                deposit_return_stake_vkey_file=selected_pool_users[i].stake.vkey_file,
            )
            for i in range(len(selected_pool_users))
        ]
        actions_num = len(update_actions)
        reqc.cip031a_01.success()
        pool_user_payment_files = [pool_user.payment.skey_file for pool_user in selected_pool_users ]
        tx_files = clusterlib.TxFiles(
            certificate_files=[r.auth_cert for r in cc_auth_records],
            proposal_files=[update_action.action_file for update_action in update_actions],
            signing_key_files=[
                *[r.cold_key_pair.skey_file for r in cc_auth_records],
            ] + pool_user_payment_files ,
        )
        address_utxos = [cluster.g_query.get_utxo(pool_user.payment.address) for pool_user in selected_pool_users]
        flatenned_utxos = list(chain.from_iterable(address_utxos))
        if conway_common.is_in_bootstrap(cluster_obj=cluster):
            with pytest.raises((clusterlib.CLIError, submit_api.SubmitApiError)) as excinfo:
                clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_bootstrap",
                    src_address=selected_pool_users[0].payment.address,
                    txins=flatenned_utxos,
                    use_build_cmd=use_build_cmd,
                    tx_files=tx_files,
                    deposit=deposit_amt,
                )
            err_str = str(excinfo.value)
            assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
            return

        reqc.cip007.start(url=helpers.get_vcs_link())
        print(f"\nSubmitting {actions_num} propals of committee size {cc_size}")
        try: 
            tx_output = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=selected_pool_users[0].payment.address,
                use_build_cmd=use_build_cmd,
                tx_files=tx_files,
                deposit=deposit_amt,
            )
        
            combined_deposit_amt = deposit_amt * len(selected_pool_users)
            out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos, address=selected_pool_users[0].payment.address)[0].amount
                == clusterlib.calculate_utxos_balance(tx_output.txins) - tx_output.fee - combined_deposit_amt
            ), f"Incorrect balance for source address `{selected_pool_users[0].payment.address}`"

            action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output.out_file)
            gov_state = cluster.g_conway_governance.query.gov_state()
            for action_ix in range(actions_num):
                prop = governance_utils.lookup_proposal(gov_state=gov_state, action_txid=action_txid, action_ix= action_ix)
                assert prop, "Update committee action not found"
                assert (
                    prop["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.UPDATE_COMMITTEE.value
                ), "Incorrect action tag"
                cc_key_hashes = {f"keyHash-{c.key_hash}" for c in cc_auth_records}
                prop_cc_key_hashes = set(prop["proposalProcedure"]["govAction"]["contents"][2].keys())
                assert cc_key_hashes == prop_cc_key_hashes, "Incorrect CC key hashes"

            # vote 

            ## CC members cannot vote on update committee action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                        conway_common._cast_vote(
                            temp_template=f"{temp_template}_no",
                            action_ix=action_ix,
                            action_txid=action_txid,
                            governance_data=governance_data,
                            cluster=cluster,
                            pool_user=selected_pool_users[0],
                            vote=conway_common.Votes.MAJORITY,
                            vote_cc=True,
                        )
                err_str = str(excinfo.value)
                assert "CommitteeVoter" in err_str, err_str
            
            actions = [
                    {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                    {"action_ix": 1, "vote": conway_common.Votes.INSUFFICIENT},
                    {"action_ix": 2, "vote": conway_common.Votes.MAJORITY},
                ]
            
            for action in actions:
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_no",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_spo=True,
                    vote_drep= True
                )
            # Check ratification
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            rat_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
            ) 
            rat_action = governance_utils.lookup_ratified_actions(
                gov_state=rat_gov_state, action_txid=action_txid, action_ix=2
            )
            assert rat_action, f"Action {action_txid}#2 not ratified"

            # Check enactment 
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            ) 
            assert enact_gov_state["committee"]["threshold"] == 0.5, "Incorrect threshold value"
            
            enact_prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                gov_state=enact_gov_state,
            )
            assert enact_prev_action_rec.txid == action_txid, "Incorrect previous action TxId"
            assert enact_prev_action_rec.ix == 2, "Incorrect previous action TxId"
            reqc.cip007.success()
            
            reqc.cip007.success()
        
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {len(update_actions)} proposals for updating a committee of size {cc_size}")
                return
    
    @allure.link(helpers.get_vcs_link())
    @common.PARAM_USE_BUILD_CMD
    @pytest.mark.smoke
    @pytest.mark.load_test
    def test_update_committee_action_equal(
        self,
        pool_users_lg: clusterlib.PoolUser,
        cluster_lock_governance: governance_setup.GovClusterT,
        use_build_cmd: bool,
    ):
        """Test update committee action.

        * create 3 proposals to add CC Members with different thresholds 
        * vote insufficiently to disapprove action 3
        * vote majority equally on actions 1 and 2
        * check that the first action on the proposal list with enough votes is enacted 
        """
        num_pool_users = 3
        cc_size = 50
        cluster, governance_data = cluster_lock_governance
        selected_pool_users = pool_users_lg[:num_pool_users]
        temp_template = common.get_test_id(cluster)
        cc_auth_records = [
            governance_utils.get_cc_member_auth_record(
                cluster_obj=cluster,
                name_template=f"{temp_template}_{i}",
            )
            for i in range(1, cc_size + 1)
        ]
        cc_members = [
            clusterlib.CCMember(
                epoch=10_000,
                cold_vkey_file=r.cold_key_pair.vkey_file,
                cold_skey_file=r.cold_key_pair.skey_file,
                hot_vkey_file=r.hot_key_pair.vkey_file,
                hot_skey_file=r.hot_key_pair.skey_file,
            )
            for r in cc_auth_records
        ]

        deposit_amt = cluster.conway_genesis["govActionDeposit"]
        anchor_url = "http://www.cc-update.com"
        anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"
        prev_action_rec = governance_utils.get_prev_action(
            action_type=governance_utils.PrevGovActionIds.COMMITTEE,
            gov_state=cluster.g_conway_governance.query.gov_state(),
        )
        
        thresholds = ["2/3","0/1","1/2"]
        reqc.cip031a_01.start(url=helpers.get_vcs_link())
        update_actions = [
            cluster.g_conway_governance.action.update_committee(
                action_name=f"{temp_template}_{i}",
                deposit_amt=deposit_amt,
                anchor_url=f"http://www.cc-update-{i}.com",
                anchor_data_hash=anchor_data_hash,
                threshold=thresholds[i],
                add_cc_members=cc_members,
                prev_action_txid=prev_action_rec.txid,
                prev_action_ix=prev_action_rec.ix,
                deposit_return_stake_vkey_file=selected_pool_users[i].stake.vkey_file,
            )
            for i in range(len(selected_pool_users))
        ]
        actions_num = len(update_actions)
        reqc.cip031a_01.success()
        pool_user_payment_files = [pool_user.payment.skey_file for pool_user in selected_pool_users ]
        tx_files = clusterlib.TxFiles(
            certificate_files=[r.auth_cert for r in cc_auth_records],
            proposal_files=[update_action.action_file for update_action in update_actions],
            signing_key_files=[
                *[r.cold_key_pair.skey_file for r in cc_auth_records],
            ] + pool_user_payment_files ,
        )
        address_utxos = [cluster.g_query.get_utxo(pool_user.payment.address) for pool_user in selected_pool_users]
        flatenned_utxos = list(chain.from_iterable(address_utxos))
        if conway_common.is_in_bootstrap(cluster_obj=cluster):
            with pytest.raises((clusterlib.CLIError, submit_api.SubmitApiError)) as excinfo:
                clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_bootstrap",
                    src_address=selected_pool_users[0].payment.address,
                    txins=flatenned_utxos,
                    use_build_cmd=use_build_cmd,
                    tx_files=tx_files,
                    deposit=deposit_amt,
                )
            err_str = str(excinfo.value)
            assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
            return

        reqc.cip007.start(url=helpers.get_vcs_link())
        print(f"\nSubmitting {actions_num} propals of committee size {cc_size}")
        try: 
            tx_output = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=selected_pool_users[0].payment.address,
                use_build_cmd=use_build_cmd,
                tx_files=tx_files,
                deposit=deposit_amt,
            )
        
            combined_deposit_amt = deposit_amt * len(selected_pool_users)
            out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos, address=selected_pool_users[0].payment.address)[0].amount
                == clusterlib.calculate_utxos_balance(tx_output.txins) - tx_output.fee - combined_deposit_amt
            ), f"Incorrect balance for source address `{selected_pool_users[0].payment.address}`"

            action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output.out_file)
            gov_state = cluster.g_conway_governance.query.gov_state()
            for action_ix in range(actions_num):
                prop = governance_utils.lookup_proposal(gov_state=gov_state, action_txid=action_txid, action_ix= action_ix)
                assert prop, "Update committee action not found"
                assert (
                    prop["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.UPDATE_COMMITTEE.value
                ), "Incorrect action tag"
                cc_key_hashes = {f"keyHash-{c.key_hash}" for c in cc_auth_records}
                prop_cc_key_hashes = set(prop["proposalProcedure"]["govAction"]["contents"][2].keys())
                assert cc_key_hashes == prop_cc_key_hashes, "Incorrect CC key hashes"

            # vote 

            ## CC members cannot vote on update committee action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                        conway_common._cast_vote(
                            temp_template=f"{temp_template}_no",
                            action_ix=action_ix,
                            action_txid=action_txid,
                            governance_data=governance_data,
                            cluster=cluster,
                            pool_user=selected_pool_users[0],
                            vote=conway_common.Votes.MAJORITY,
                            vote_cc=True,
                        )
                err_str = str(excinfo.value)
                assert "CommitteeVoter" in err_str, err_str
            
            actions = [
                    {"action_ix": 0, "vote": conway_common.Votes.MAJORITY},
                    {"action_ix": 1, "vote": conway_common.Votes.MAJORITY},
                    {"action_ix": 2, "vote": conway_common.Votes.INSUFFICIENT},
                ]
            
            for action in actions:
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_no",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_spo=True,
                    vote_drep= True
                )

            # Check ratification
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            rat_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
            ) 
            rat_action = governance_utils.lookup_ratified_actions(
                gov_state=rat_gov_state, action_txid=action_txid, action_ix=0
            )
            assert rat_action, f"Action {action_txid}#0 not ratified"

            # Check enactment 
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            ) 
            assert enact_gov_state["committee"]["threshold"]["denominator"] == 3, "Incorrect threshold denominator value"
            assert enact_gov_state["committee"]["threshold"]["numerator"] == 2, "Incorrect threshold numerator value"            
                        
            enact_prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                gov_state=enact_gov_state,
            )
            assert enact_prev_action_rec.txid == action_txid, "Incorrect previous action TxId"
            assert enact_prev_action_rec.ix == 0, "Incorrect previous action TxId"
            reqc.cip007.success()

        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {len(update_actions)} proposals for updating a committee of size {cc_size}")
                return           

    @allure.link(helpers.get_vcs_link())
    @common.PARAM_USE_BUILD_CMD
    @pytest.mark.smoke
    @pytest.mark.load_test
    def test_update_committee_action_insufficient(
        self,
        pool_users_lg: clusterlib.PoolUser,
        cluster_lock_governance: governance_setup.GovClusterT,
        use_build_cmd: bool,
    ):
        """Test update committee action.

        * create 3 proposals to add CC Members with different thresholds 
        * vote insufficiently to disapprove all proposals
        * check that the proposed changes are not ratified or enacted
        """
        num_pool_users = 3
        cc_size = 50
        cluster, governance_data = cluster_lock_governance
        selected_pool_users = pool_users_lg[:num_pool_users]
        temp_template = common.get_test_id(cluster)
        cc_auth_records = [
            governance_utils.get_cc_member_auth_record(
                cluster_obj=cluster,
                name_template=f"{temp_template}_{i}",
            )
            for i in range(1, cc_size + 1)
        ]
        cc_members = [
            clusterlib.CCMember(
                epoch=10_000,
                cold_vkey_file=r.cold_key_pair.vkey_file,
                cold_skey_file=r.cold_key_pair.skey_file,
                hot_vkey_file=r.hot_key_pair.vkey_file,
                hot_skey_file=r.hot_key_pair.skey_file,
            )
            for r in cc_auth_records
        ]

        deposit_amt = cluster.conway_genesis["govActionDeposit"]
        anchor_url = "http://www.cc-update.com"
        anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"
        init_gov_state = cluster.g_conway_governance.query.gov_state()
        prev_action_rec = governance_utils.get_prev_action(
            action_type=governance_utils.PrevGovActionIds.COMMITTEE,
            gov_state=init_gov_state,
        )
        prev_threshold = init_gov_state["committee"]["threshold"]
        thresholds = ["2/3","0/1","1/2"]
        reqc.cip031a_01.start(url=helpers.get_vcs_link())
        update_actions = [
            cluster.g_conway_governance.action.update_committee(
                action_name=f"{temp_template}_{i}",
                deposit_amt=deposit_amt,
                anchor_url=f"http://www.cc-update-{i}.com",
                anchor_data_hash=anchor_data_hash,
                threshold=thresholds[i],
                add_cc_members=cc_members,
                prev_action_txid=prev_action_rec.txid,
                prev_action_ix=prev_action_rec.ix,
                deposit_return_stake_vkey_file=selected_pool_users[i].stake.vkey_file,
            )
            for i in range(len(selected_pool_users))
        ]
        actions_num = len(update_actions)
        reqc.cip031a_01.success()
        pool_user_payment_files = [pool_user.payment.skey_file for pool_user in selected_pool_users ]
        tx_files = clusterlib.TxFiles(
            certificate_files=[r.auth_cert for r in cc_auth_records],
            proposal_files=[update_action.action_file for update_action in update_actions],
            signing_key_files=[
                *[r.cold_key_pair.skey_file for r in cc_auth_records],
            ] + pool_user_payment_files ,
        )
        address_utxos = [cluster.g_query.get_utxo(pool_user.payment.address) for pool_user in selected_pool_users]
        flatenned_utxos = list(chain.from_iterable(address_utxos))
        if conway_common.is_in_bootstrap(cluster_obj=cluster):
            with pytest.raises((clusterlib.CLIError, submit_api.SubmitApiError)) as excinfo:
                clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_bootstrap",
                    src_address=selected_pool_users[0].payment.address,
                    txins=flatenned_utxos,
                    use_build_cmd=use_build_cmd,
                    tx_files=tx_files,
                    deposit=deposit_amt,
                )
            err_str = str(excinfo.value)
            assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
            return

        reqc.cip007.start(url=helpers.get_vcs_link())
        print(f"\nSubmitting {actions_num} proposals of committee size {cc_size}")
        try: 
            tx_output = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=selected_pool_users[0].payment.address,
                use_build_cmd=use_build_cmd,
                tx_files=tx_files,
                deposit=deposit_amt,
            )
        
            combined_deposit_amt = deposit_amt * len(selected_pool_users)
            out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos, address=selected_pool_users[0].payment.address)[0].amount
                == clusterlib.calculate_utxos_balance(tx_output.txins) - tx_output.fee - combined_deposit_amt
            ), f"Incorrect balance for source address `{selected_pool_users[0].payment.address}`"

            action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output.out_file)
            gov_state = cluster.g_conway_governance.query.gov_state()
            for action_ix in range(actions_num):
                prop = governance_utils.lookup_proposal(gov_state=gov_state, action_txid=action_txid, action_ix= action_ix)
                assert prop, "Update committee action not found"
                assert (
                    prop["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.UPDATE_COMMITTEE.value
                ), "Incorrect action tag"
                cc_key_hashes = {f"keyHash-{c.key_hash}" for c in cc_auth_records}
                prop_cc_key_hashes = set(prop["proposalProcedure"]["govAction"]["contents"][2].keys())
                assert cc_key_hashes == prop_cc_key_hashes, "Incorrect CC key hashes"

            # vote 

            ## CC members cannot vote on update committee action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                        conway_common._cast_vote(
                            temp_template=f"{temp_template}_no",
                            action_ix=action_ix,
                            action_txid=action_txid,
                            governance_data=governance_data,
                            cluster=cluster,
                            pool_user=selected_pool_users[0],
                            vote=conway_common.Votes.MAJORITY,
                            vote_cc=True,
                        )
                err_str = str(excinfo.value)
                assert "CommitteeVoter" in err_str, err_str
            
            actions = [
                    {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                    {"action_ix": 1, "vote": conway_common.Votes.INSUFFICIENT},
                    {"action_ix": 2, "vote": conway_common.Votes.INSUFFICIENT},
                ]
            
            for action in actions:
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_no",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_spo=True,
                    vote_drep= True
                )

            # Check ratification
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            rat_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
            ) 
            for action_ix in range(actions_num):
                rat_action = governance_utils.lookup_ratified_actions(
                    gov_state=rat_gov_state, action_txid=action_txid, action_ix=action_ix
                )
                assert not rat_action, f"Action {action_txid}#{action_ix} ratified without enough votes"

            # Check enactment 
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            ) 
            assert enact_gov_state["committee"]["threshold"] == prev_threshold, "Incorrect threshold value"
            enact_prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                gov_state=enact_gov_state,
            )
            assert enact_prev_action_rec.txid == prev_action_rec.txid, "Incorrect previous action TxId"
            assert enact_prev_action_rec.ix == prev_action_rec.ix, "Incorrect previous action TxId"
            
            reqc.cip007.success()
        
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {len(update_actions)} proposals for updating a committee of size {cc_size}")
                return 