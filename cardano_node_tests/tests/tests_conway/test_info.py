"""Tests for Conway governance info."""

# pylint: disable=expression-not-assigned
from itertools import chain
import logging
import typing as tp

import allure
import pytest
from cardano_clusterlib import clusterlib
from cardano_clusterlib.query_group import QueryGroup

from cardano_node_tests.cluster_management import cluster_management
from cardano_node_tests.tests import common
from cardano_node_tests.tests import reqs_conway as reqc
from cardano_node_tests.tests.tests_conway import conway_common
from cardano_node_tests.utils import clusterlib_utils
from cardano_node_tests.utils import configuration
from cardano_node_tests.utils import dbsync_utils
from cardano_node_tests.utils import governance_setup
from cardano_node_tests.utils import governance_utils
from cardano_node_tests.utils import helpers
from cardano_node_tests.utils.versions import VERSIONS

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(
    VERSIONS.transaction_era < VERSIONS.CONWAY,
    reason="runs only with Tx era >= Conway",
)


@pytest.fixture
def pool_users_ug(
    cluster_manager: cluster_management.ClusterManager,
    cluster_use_governance: governance_setup.GovClusterT,
) -> clusterlib.PoolUser:
    """Create a pool user for "use governance"."""
    cluster, __ = cluster_use_governance
    key = helpers.get_current_line_str()
    name_template = common.get_test_id(cluster)
    return conway_common.get_registered_pool_user(
        cluster_manager=cluster_manager,
        name_template=name_template,
        cluster_obj=cluster,
        caching_key=key,
    )

class TestInfo:
    """Tests for info."""
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.load_test
    def test_info(
        self,
        cluster_manager: cluster_management.ClusterManager,
        cluster_use_governance: governance_setup.GovClusterT,
        pool_users_ug: clusterlib.PoolUser,
    ):
        """Test voting on info action.

        * submit an "info" action
        * 3 SPOs, 100 DReps and 90 CC Members vote on the action
        * check the votes
        """
        # pylint: disable=too-many-locals,too-many-statements        
        cluster, governance_data = cluster_use_governance
        temp_template = common.get_test_id(cluster)
        action_deposit_amt = cluster.conway_genesis["govActionDeposit"]
        actions_num = 3
        pool_user_ug = pool_users_ug[0]

        # Create an action
        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli016, reqc.cip031a_03, reqc.cip054_06)]
        info_actions = [
            cluster.g_conway_governance.action.create_info(
                action_name= f"{temp_template}_{i}",
                deposit_amt= action_deposit_amt,
                anchor_url= f"http://www.info-action-{i}.com",
                anchor_data_hash= "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d",
                deposit_return_stake_vkey_file=pool_user_ug.stake.vkey_file,
            )
            for i in range(actions_num)
        ]
        print(f"\nSubmitting {len(info_actions)} info action proposals in a single transaction")
        [r.success() for r in (reqc.cli016, reqc.cip031a_03, reqc.cip054_06)]

        tx_files_action = clusterlib.TxFiles(
            proposal_files=[info_action.action_file for info_action in info_actions],
            signing_key_files=[pool_user_ug.payment.skey_file],
        )

        # Make sure we have enough time to submit the proposal in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
        )
        reqc.cli023.start(url=helpers.get_vcs_link())
        
        try:
            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_user_ug.payment.address,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )
            reqc.cli023.success()
            actions_deposit_combined = action_deposit_amt * actions_num
            out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_users_ug[0].payment.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_action.txins)
                - tx_output_action.fee
                - actions_deposit_combined
            ), f"Incorrect balance for source address `{pool_users_ug[0].payment.address}`"

            action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output_action.out_file)
            reqc.cli031.start(url=helpers.get_vcs_link())
            action_gov_state = cluster.g_conway_governance.query.gov_state()
            _cur_epoch = cluster.g_query.get_epoch()
            conway_common.save_gov_state(
                gov_state=action_gov_state, name_template=f"{temp_template}_action_{_cur_epoch}"
            )
            
            for ix in range(actions_num):
                prop_action = governance_utils.lookup_proposal(
                    gov_state=action_gov_state, action_txid=action_txid, action_ix=ix
                )
                reqc.cli031.success()
                assert prop_action, f"Info action not found for index {ix}"
                assert (
                    prop_action["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.INFO_ACTION.value
                ), "Incorrect action tag"

            # Vote            
            print(f"{len(governance_data.pools_cold)} SPOs are voting")
            print(f"{len(governance_data.cc_members)} CC members are voting")
            print(f"{len(governance_data.dreps_reg)} DReps are voting")
            
            _url = helpers.get_vcs_link()
            [r.start(url=_url) for r in (reqc.cli021, reqc.cip053, reqc.cip059)]
            votes_cc = []
            votes_drep=[]
            votes_spo=[]

            for action_ix in range(actions_num):
                votes_cc.extend( 
                    [
                        cluster.g_conway_governance.vote.create_committee(
                            vote_name=f"{temp_template}_{action_ix}_cc{i}",
                            action_txid=action_txid,
                            action_ix=action_ix,
                            vote=clusterlib.Votes.YES,
                            cc_hot_vkey_file=m.hot_vkey_file,
                        )
                        for i, m in enumerate(governance_data.cc_members, start=1)
                    ]
                )
                votes_drep.extend( 
                    [
                        cluster.g_conway_governance.vote.create_drep(
                            vote_name=f"{temp_template}_{action_ix}_drep{i}",
                            action_txid=action_txid,
                            action_ix=action_ix,
                            vote=clusterlib.Votes.YES,
                            drep_vkey_file=d.key_pair.vkey_file,
                        )
                        for i, d in enumerate(governance_data.dreps_reg, start=1)
                    ]
                )
                votes_spo.extend( 
                    [
                        cluster.g_conway_governance.vote.create_spo(
                            vote_name=f"{temp_template}_{action_ix}__pool{i}",
                            action_txid=action_txid,
                            action_ix=action_ix,
                            vote=clusterlib.Votes.YES,
                            cold_vkey_file=p.vkey_file,
                        )
                        for i, p in enumerate(governance_data.pools_cold, start=1)
                    ]
                )
            [r.success() for r in (reqc.cli021, reqc.cip059)]

            # Make sure we have enough time to submit the votes in one epoch
            clusterlib_utils.wait_for_epoch_interval(
                cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
            )

            reqc.cli024.start(url=helpers.get_vcs_link())
            cc_keys = [r.hot_skey_file for r in governance_data.cc_members]
            drep_keys = [r.key_pair.skey_file for r in governance_data.dreps_reg]
            spo_keys = [r.skey_file for r in governance_data.pools_cold]
            
            # submit cc vote
            print(f"submitting {len(votes_cc)} CC votes")
            cc_vote_output = conway_common.submit_vote_(
                cluster_obj=cluster,
                name_template=temp_template,
                payment_addr=pool_user_ug.payment,
                votes=votes_cc,
                keys=cc_keys,
                use_build_cmd=True,
            )
            print(f"submitting {len(votes_drep)} DRep votes")
            drep_vote_output = conway_common.submit_vote_(
                cluster_obj=cluster,
                name_template=temp_template,
                payment_addr=pool_user_ug.payment,
                votes=votes_drep,
                keys=drep_keys,
                use_build_cmd=True,
            )
            print(f"submitting {len(votes_spo)} SPO votes")
            spo_vote_output = conway_common.submit_vote_(
                cluster_obj=cluster,
                name_template=temp_template,
                payment_addr=pool_user_ug.payment,
                votes=votes_spo,
                keys=spo_keys,
                use_build_cmd=True,
            )
            reqc.cli024.success()

            def flatten_and_collect_txids(vote_tx_outputs, cluster):
                collected_txids = []
                def flatten_and_collect(elements):
                    nonlocal collected_txids
                    for element in elements:
                        if isinstance(element, list):
                            flatten_and_collect(element)
                        else:
                            vote_txid = cluster.g_transaction.get_txid(tx_body_file=element.out_file)
                            collected_txids.append(vote_txid)
                flatten_and_collect(vote_tx_outputs)
                return collected_txids

            vote_tx_outputs = [cc_vote_output, drep_vote_output, spo_vote_output]
            vote_txids = flatten_and_collect_txids(vote_tx_outputs, cluster)
            vote_gov_state = cluster.g_conway_governance.query.gov_state()
            _cur_epoch = cluster.g_query.get_epoch()
            conway_common.save_gov_state(
                gov_state=vote_gov_state, name_template=f"{temp_template}_vote_{_cur_epoch}"
            )

            for action_ix in range(actions_num):
                prop_vote = governance_utils.lookup_proposal(
                    gov_state=vote_gov_state, action_txid=action_txid, action_ix=action_ix
                )
                assert not configuration.HAS_CC or prop_vote["committeeVotes"], "No committee votes"
                assert prop_vote["dRepVotes"], "No DRep votes"
                assert prop_vote["stakePoolVotes"], "No stake pool votes"

            # Check that the Info action cannot be ratified
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            approved_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=approved_gov_state, name_template=f"{temp_template}_approved_{_cur_epoch}"
            )
            
            rat_info_actions=[]
            for action_ix in range(actions_num):
                rat_info_actions.append
                (
                    governance_utils.lookup_ratified_actions
                    (
                        gov_state=approved_gov_state,
                        action_txid=action_txid,
                        action_ix=action_ix,
                    )
                )
            for rat_info_action in rat_info_actions:
                assert not rat_info_action, "Action found in ratified actions"
            
            reqc.cip053.success()

            reqc.cip038_05.start(url=helpers.get_vcs_link())
            assert not approved_gov_state["nextRatifyState"][
                "ratificationDelayed"
            ], "Ratification is delayed unexpectedly"
            reqc.cip038_05.success()

            # Check the last action view 
            governance_utils.check_action_view(cluster_obj=cluster, action_data=info_actions[0])

            # Check vote view
            reqc.cli022.start(url=helpers.get_vcs_link())
            if votes_cc:
                governance_utils.check_vote_view(cluster_obj=cluster, vote_data=votes_cc[0])
            governance_utils.check_vote_view(cluster_obj=cluster, vote_data=votes_drep[0])
            governance_utils.check_vote_view(cluster_obj=cluster, vote_data=votes_spo[0])
            reqc.cli022.success()

            # Check dbsync
            for vote_txid in vote_txids:
                dbsync_utils.check_votes(
                    votes=governance_utils.VotedVotes(cc=votes_cc, drep=votes_drep, spo=votes_spo),
                    txhash=vote_txid,
                )
            cluster_manager.set_needs_respin()        

        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {len(info_actions)} proposals for info action in a single transaction")
                return