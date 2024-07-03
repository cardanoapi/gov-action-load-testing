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
        no_of_users= 300
    )

class TestInfo:
    """Tests for info."""
    @pytest.mark.parametrize("num_pool_users", reversed(range(116, 120)))
    @allure.link(helpers.get_vcs_link())
    def test_info(
        self,
        cluster_manager: cluster_management.ClusterManager,
        cluster_use_governance: governance_setup.GovClusterT,
        pool_users_ug: clusterlib.PoolUser,
        num_pool_users: int,
    ):
        """Test voting on info action.

        * submit an "info" action
        * vote on the action
        * check the votes
        """
        # pylint: disable=too-many-locals,too-many-statements        
        cluster, governance_data = cluster_use_governance
        temp_template = common.get_test_id(cluster)
        action_deposit_amt = cluster.conway_genesis["govActionDeposit"]
        
        pool_users_ug = pool_users_ug[:num_pool_users]
        total_participants = len(pool_users_ug)
        
        # Create an action
        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli016, reqc.cip031a_03, reqc.cip054_06)]
        info_actions = [
            cluster.g_conway_governance.action.create_info(
                action_name= temp_template,
                deposit_amt= action_deposit_amt,
                anchor_url= f"http://www.info-action-{i}.com",
                anchor_data_hash= "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d",
                deposit_return_stake_vkey_file=pool_users_ug[i].stake.vkey_file,
            )
            for i in range(total_participants)
        ]
        print(f"\n{len(info_actions)} proposals for info action are being submitted in a single transaction")
        [r.success() for r in (reqc.cli016, reqc.cip031a_03, reqc.cip054_06)]

        tx_files_action = clusterlib.TxFiles(
            proposal_files=[info_action.action_file for info_action in info_actions],
            signing_key_files=[pool_user_ug.payment.skey_file for pool_user_ug in pool_users_ug],
        )

        # Make sure we have enough time to submit the proposal in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
        )
        address_utxos = [cluster.g_query.get_utxo(pool_user_ug.payment.address) for pool_user_ug in pool_users_ug]
        flatenned_utxos = list(chain.from_iterable(address_utxos))
        reqc.cli023.start(url=helpers.get_vcs_link())
        
        try:
            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_users_ug[0].payment.address,
                txins=flatenned_utxos,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {len(info_actions)} proposals in a single transaction")
                return

        reqc.cli023.success()

        out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
        assert (
            clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_users_ug[0].payment.address)[
                0
            ].amount
            == clusterlib.calculate_utxos_balance(tx_output_action.txins)
            - tx_output_action.fee
            - (action_deposit_amt)
        ), f"Incorrect balance for source address `{pool_users_ug[0].payment.address}`"

        action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output_action.out_file)
        reqc.cli031.start(url=helpers.get_vcs_link())
        action_gov_state = cluster.g_conway_governance.query.gov_state()
        _cur_epoch = cluster.g_query.get_epoch()
        conway_common.save_gov_state(
            gov_state=action_gov_state, name_template=f"{temp_template}_action_{_cur_epoch}"
        )
        prop_action = governance_utils.lookup_proposal(
            gov_state=action_gov_state, action_txid=action_txid
        )
        reqc.cli031.success()
        assert prop_action, "Info action not found"
        assert (
            prop_action["proposalProcedure"]["govAction"]["tag"]
            == governance_utils.ActionTags.INFO_ACTION.value
        ), "Incorrect action tag"

        # Vote
        action_ix = prop_action["actionId"]["govActionIx"]

        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli021, reqc.cip053, reqc.cip059)]
        print(len(governance_data.cc_members), " CC members are voting")
        print(len(governance_data.dreps_reg), " DReps are voting")
        print(len(governance_data.pools_cold), " SPOs are voting")
        votes_cc = [
            cluster.g_conway_governance.vote.create_committee(
                vote_name=f"{temp_template}_cc{i}",
                action_txid=action_txid,
                action_ix=action_ix,
                vote=clusterlib.Votes.YES,
                cc_hot_vkey_file=m.hot_vkey_file,
            )
            for i, m in enumerate(governance_data.cc_members, start=1)
        ]
        votes_drep = [
            cluster.g_conway_governance.vote.create_drep(
                vote_name=f"{temp_template}_drep{i}",
                action_txid=action_txid,
                action_ix=action_ix,
                vote=clusterlib.Votes.YES,
                drep_vkey_file=d.key_pair.vkey_file,
            )
            for i, d in enumerate(governance_data.dreps_reg, start=1)
        ]
        votes_spo = [
            cluster.g_conway_governance.vote.create_spo(
                vote_name=f"{temp_template}_pool{i}",
                action_txid=action_txid,
                action_ix=action_ix,
                vote=clusterlib.Votes.YES,
                cold_vkey_file=p.vkey_file,
            )
            for i, p in enumerate(governance_data.pools_cold, start=1)
        ]
        [r.success() for r in (reqc.cli021, reqc.cip059)]

        votes: tp.List[governance_utils.VotesAllT] = [*votes_cc, *votes_drep, *votes_spo]
        vote_keys = [
            *[r.hot_skey_file for r in governance_data.cc_members],
            *[r.key_pair.skey_file for r in governance_data.dreps_reg],
            *[r.skey_file for r in governance_data.pools_cold],
        ]

        # Make sure we have enough time to submit the votes in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
        )

        reqc.cli024.start(url=helpers.get_vcs_link())
        vote_tx_outputs = conway_common.submit_vote(
            cluster_obj=cluster,
            name_template=temp_template,
            payment_addr=pool_users_ug[0].payment,
            votes=votes,
            keys=vote_keys,
            use_build_cmd=True,
        )
        reqc.cli024.success()

        vote_txids = [cluster.g_transaction.get_txid(tx_body_file=vote_tx_output.out_file) for vote_tx_output in vote_tx_outputs]

        vote_gov_state = cluster.g_conway_governance.query.gov_state()
        _cur_epoch = cluster.g_query.get_epoch()
        conway_common.save_gov_state(
            gov_state=vote_gov_state, name_template=f"{temp_template}_vote_{_cur_epoch}"
        )
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
        rat_info_action = governance_utils.lookup_ratified_actions(
            gov_state=approved_gov_state,
            action_txid=action_txid,
            action_ix=action_ix,
        )
        assert not rat_info_action, "Action found in ratified actions"
        reqc.cip053.success()

        reqc.cip038_05.start(url=helpers.get_vcs_link())
        assert not approved_gov_state["nextRatifyState"][
            "ratificationDelayed"
        ], "Ratification is delayed unexpectedly"
        reqc.cip038_05.success()

        # Check the last action view 
        governance_utils.check_action_view(cluster_obj=cluster, action_data=info_actions[total_participants-1])

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