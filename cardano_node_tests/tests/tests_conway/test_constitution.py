"""Tests for Conway governance constitution."""

# pylint: disable=expression-not-assigned
import logging

import allure
import pytest
from cardano_clusterlib import clusterlib
import typing as tp 
from cardano_node_tests.cluster_management import cluster_management
from cardano_node_tests.tests import common
from cardano_node_tests.tests import issues
from cardano_node_tests.tests import reqs_conway as reqc
from cardano_node_tests.tests.tests_conway import conway_common
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
def pool_users_lg(
    cluster_manager: cluster_management.ClusterManager,
    cluster_lock_governance: governance_setup.GovClusterT,
) -> clusterlib.PoolUser:
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


class TestConstitution:
    """Tests for constitution."""

    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_change_constitution_majority(
        self,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: tp.List[clusterlib.PoolUser],
    ):
        """Test enactment of change of constitution.

        * submit 3 "create constitution" actions
        * check that SPOs cannot vote on a "create constitution" action
        * 90 CC members and 100 DReps vote insufficiently to disapprove actions 1 and 2
        * 90 CC members and 100 DReps vote majority to approve action 3
        * check that action 3 is ratified
        * try to disapprove the ratified action, this shouldn't have any effect
        * check that action 3 is enacted
        * check that it's not possible to vote on enacted action
        """
        num_pool_users= 3
        pool_users_lg=pool_users_lg[:num_pool_users]
        total_participants = len(pool_users_lg)
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_lock_governance
        temp_template = common.get_test_id(cluster)

        # Create an action

        anchor_url = "http://www.const-action.com"

        constitution_file = f"{temp_template}_constitution.txt"
        constitution_text = (
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
            "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi "
            "ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit "
            "in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
            "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia "
            "deserunt mollit anim id est laborum."
        )
        with open(constitution_file, "w", encoding="utf-8") as out_fp:
            out_fp.write(constitution_text)

        constitution_url = "http://www.const-new.com"
        reqc.cli002.start(url=helpers.get_vcs_link())
        constitution_hash = cluster.g_conway_governance.get_anchor_data_hash(
            file_text=constitution_file
        )
        reqc.cli002.success()
        try:
            if conway_common.is_in_bootstrap(cluster_obj=cluster):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common.propose_change_constitution(
                        cluster_obj=cluster,
                        name_template=f"{temp_template}_constitution_bootstrap",
                        constitution_hash=constitution_hash,
                        pool_users=pool_users_lg,
                    )
                err_str = str(excinfo.value)
                assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
                return
            print(f"\n{total_participants} proposals for update constitution action are being submitted in a single transaction")
            _url = helpers.get_vcs_link()
            [r.start(url=_url) for r in (reqc.cli013, reqc.cip031a_02, reqc.cip031c_01, reqc.cip054_03)]
            (
                constitution_actions,
                action_txid,
                action_ixs,
            ) = conway_common.propose_change_constitution(
                cluster_obj=cluster,
                name_template=f"{temp_template}_constitution",
                constitution_hash=constitution_hash,
                pool_users=pool_users_lg,
            )
            [r.success() for r in (reqc.cli013, reqc.cip031a_02, reqc.cip031c_01, reqc.cip054_03)]
            actions_num = len(constitution_actions)
            for action_ix in action_ixs:
                # Check that SPOs cannot vote on change of constitution action
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_with_spos",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_spo=True,
                    )
                err_str = str(excinfo.value)
                assert "StakePoolVoter" in err_str, err_str

            for action_ix in action_ixs:
                # Vote & disapprove the action
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.INSUFFICIENT,
                        vote_cc=True,
                        vote_drep=True,
                )

            print(len(governance_data.cc_members), " CC members are voting")
            print(len(governance_data.dreps_reg), " DReps are voting")
            
            ## disapprove action 1 and 2 
            ## approve action 3
            actions = [
                {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 1, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 2, "vote": conway_common.Votes.MAJORITY},
            ]

            # Vote & approve the action
            reqc.cip042.start(url=helpers.get_vcs_link())
            for action in actions:
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_cc=True,
                    vote_drep=True,
                )

            def _assert_anchor(anchor: dict):
                assert (
                    anchor["dataHash"]
                    == constitution_hash
                    == "d6d9034f61e2f7ada6e58c252e15684c8df7f0b197a95d80f42ca0a3685de26e"
                ), "Incorrect constitution data hash"
                assert anchor["url"] == "http://www.const-new-2.com", "Incorrect constitution data URL"

            def _check_state(state: dict):
                anchor = state["constitution"]["anchor"]
                _assert_anchor(anchor)

            def _check_cli_query():
                anchor = cluster.g_conway_governance.query.constitution()["anchor"]
                _assert_anchor(anchor)

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
                                   
            # Disapprove ratified action, the voting shouldn't have any effect
            conway_common._cast_vote(
                temp_template=f"{temp_template}_with_dreps",
                action_ix=2,
                action_txid=action_txid,
                governance_data=governance_data,
                cluster=cluster,
                pool_user=pool_users_lg[0],
                vote=conway_common.Votes.MAJORITY,
                vote_cc=True,
                vote_drep=True,
            )

            next_rat_state = rat_gov_state["nextRatifyState"]
            _url = helpers.get_vcs_link()
            [
                r.start(url=_url)
                for r in (
                    reqc.cli001,
                    reqc.cip001a,
                    reqc.cip001b,
                    reqc.cip072,
                    reqc.cip073_01,
                    reqc.cip073_04,
                )
            ]
            _check_state(next_rat_state["nextEnactState"])
            [r.success() for r in (reqc.cli001, reqc.cip001a, reqc.cip001b, reqc.cip073_01)]
            reqc.cip038_02.start(url=_url)
            assert next_rat_state["ratificationDelayed"], "Ratification not delayed"
            reqc.cip038_02.success()

            # Check enactment
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            )
            _check_state(enact_gov_state)
            
            enact_prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.CONSTITUTION,
                gov_state=enact_gov_state,
            )
            # assert action_ix 2 in previous action
            assert enact_prev_action_rec.txid == action_txid, "Incorrect previous action Txid"
            assert enact_prev_action_rec.ix == 2, "Incorrect previous action index"
            
            [r.success() for r in (reqc.cip042, reqc.cip072, reqc.cip073_04)]

            reqc.cli036.start(url=helpers.get_vcs_link())
            _check_cli_query()
            reqc.cli036.success()

            # Try to vote on enacted action
            with pytest.raises(clusterlib.CLIError) as excinfo:
                conway_common.cast_vote(
                    cluster_obj=cluster,
                    governance_data=governance_data,
                    name_template=f"{temp_template}_enacted",
                    payment_addr=pool_users_lg[0].payment,
                    action_txid=action_txid,
                    action_ix=2,
                    approve_cc=False,
                    approve_drep=False,
                )
            err_str = str(excinfo.value)
            assert "(GovActionsDoNotExist" in err_str, err_str

            # Check action view
            reqc.cli020.start(url=helpers.get_vcs_link())
            governance_utils.check_action_view(cluster_obj=cluster, action_data=constitution_actions[2])
            reqc.cli020.success()

        except clusterlib.CLIError as exc:
                err_str = str(exc)
                if "MaxTxSizeUTxO" in err_str:
                    print(f"Fails at proposing {(num_pool_users)} constitution update actions in a single transaction")
                    return
    
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_change_constitution_equal(
        self,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: tp.List[clusterlib.PoolUser],
    ):
        """Test enactment of change of constitution.

        * submit 3 "create constitution" actions
        * check that SPOs cannot vote on a "create constitution" action
        * 90 CC members and 100 DReps vote insufficiently to disapprove action 1
        * 90 CC members and 100 DReps vote majority to approve actions 2 and 3
        * check that the first action on the proposal list with enough votes is ratified
        * try to disapprove the ratified action, this shouldn't have any effect
        * check that the ratified action is enacted
        * check that it's not possible to vote on enacted action
        """
        num_pool_users= 3
        pool_users_lg=pool_users_lg[:num_pool_users]
        total_participants = len(pool_users_lg)
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_lock_governance
        temp_template = common.get_test_id(cluster)

        # Create an action

        anchor_url = "http://www.const-action.com"

        constitution_file = f"{temp_template}_constitution.txt"
        constitution_text = (
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
            "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi "
            "ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit "
            "in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
            "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia "
            "deserunt mollit anim id est laborum."
        )
        with open(constitution_file, "w", encoding="utf-8") as out_fp:
            out_fp.write(constitution_text)

        constitution_url = "http://www.const-new.com"
        reqc.cli002.start(url=helpers.get_vcs_link())
        constitution_hash = cluster.g_conway_governance.get_anchor_data_hash(
            file_text=constitution_file
        )
        reqc.cli002.success()
        try:
            if conway_common.is_in_bootstrap(cluster_obj=cluster):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common.propose_change_constitution(
                        cluster_obj=cluster,
                        name_template=f"{temp_template}_constitution_bootstrap",
                        constitution_hash=constitution_hash,
                        pool_users=pool_users_lg,
                    )
                err_str = str(excinfo.value)
                assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
                return
            print(f"\n{total_participants} proposals for update constitution action are being submitted in a single transaction")
            _url = helpers.get_vcs_link()
            [r.start(url=_url) for r in (reqc.cli013, reqc.cip031a_02, reqc.cip031c_01, reqc.cip054_03)]
            (
                constitution_actions,
                action_txid,
                action_ixs,
            ) = conway_common.propose_change_constitution(
                cluster_obj=cluster,
                name_template=f"{temp_template}_constitution",
                constitution_hash=constitution_hash,
                pool_users=pool_users_lg,
            )
            [r.success() for r in (reqc.cli013, reqc.cip031a_02, reqc.cip031c_01, reqc.cip054_03)]
            for action_ix in action_ixs:
                # Check that SPOs cannot vote on change of constitution action
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_with_spos",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_spo=True,
                    )
                err_str = str(excinfo.value)
                assert "StakePoolVoter" in err_str, err_str

            for action_ix in action_ixs:
                # Vote & disapprove the action
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.INSUFFICIENT,
                        vote_cc=True,
                        vote_drep=True,
                )

            print(len(governance_data.cc_members), " CC members are voting")
            print(len(governance_data.dreps_reg), " DReps are voting")
            
            ## disapprove action 1 and 2 
            ## approve action 3
            actions = [
                {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 1, "vote": conway_common.Votes.MAJORITY},
                {"action_ix": 2, "vote": conway_common.Votes.MAJORITY},
            ]

            # Vote & approve the action
            reqc.cip042.start(url=helpers.get_vcs_link())
            for action in actions:
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_cc=True,
                    vote_drep=True,
                )

            def _assert_anchor(anchor: dict):
                assert (
                    anchor["dataHash"]
                    == constitution_hash
                    == "d6d9034f61e2f7ada6e58c252e15684c8df7f0b197a95d80f42ca0a3685de26e"
                ), "Incorrect constitution data hash"
                assert anchor["url"] == "http://www.const-new-1.com", "Incorrect constitution data URL"

            def _check_state(state: dict):
                anchor = state["constitution"]["anchor"]
                _assert_anchor(anchor)

            def _check_cli_query():
                anchor = cluster.g_conway_governance.query.constitution()["anchor"]
                _assert_anchor(anchor)

            # Check ratification
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            rat_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
            ) 
            rat_action = governance_utils.lookup_ratified_actions(
                gov_state=rat_gov_state, action_txid=action_txid, action_ix=1
            )
            assert rat_action, f"Action {action_txid}#1 not ratified"
                                   
            # Disapprove ratified action, the voting shouldn't have any effect
            conway_common._cast_vote(
                temp_template=f"{temp_template}_with_dreps",
                action_ix=1,
                action_txid=action_txid,
                governance_data=governance_data,
                cluster=cluster,
                pool_user=pool_users_lg[0],
                vote=conway_common.Votes.INSUFFICIENT,
                vote_cc=True,
                vote_drep=True,
            )

            next_rat_state = rat_gov_state["nextRatifyState"]
            _url = helpers.get_vcs_link()
            [
                r.start(url=_url)
                for r in (
                    reqc.cli001,
                    reqc.cip001a,
                    reqc.cip001b,
                    reqc.cip072,
                    reqc.cip073_01,
                    reqc.cip073_04,
                )
            ]
            _check_state(next_rat_state["nextEnactState"])
            [r.success() for r in (reqc.cli001, reqc.cip001a, reqc.cip001b, reqc.cip073_01)]
            reqc.cip038_02.start(url=_url)
            assert next_rat_state["ratificationDelayed"], "Ratification not delayed"
            reqc.cip038_02.success()

            # Check enactment
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            )
            _check_state(enact_gov_state)
            
            enact_prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.CONSTITUTION,
                gov_state=enact_gov_state,
            )
            # assert action_ix 2 in previous action
            assert enact_prev_action_rec.txid == action_txid, "Incorrect previous action Txid"
            assert enact_prev_action_rec.ix == 1, "Incorrect previous action index"
            
            [r.success() for r in (reqc.cip042, reqc.cip072, reqc.cip073_04)]

            reqc.cli036.start(url=helpers.get_vcs_link())
            _check_cli_query()
            reqc.cli036.success()

            # Try to vote on enacted action
            with pytest.raises(clusterlib.CLIError) as excinfo:
                conway_common.cast_vote(
                    cluster_obj=cluster,
                    governance_data=governance_data,
                    name_template=f"{temp_template}_enacted",
                    payment_addr=pool_users_lg[0].payment,
                    action_txid=action_txid,
                    action_ix=1,
                    approve_cc=False,
                    approve_drep=False,
                )
            err_str = str(excinfo.value)
            assert "(GovActionsDoNotExist" in err_str, err_str

            # Check action view
            reqc.cli020.start(url=helpers.get_vcs_link())
            governance_utils.check_action_view(cluster_obj=cluster, action_data=constitution_actions[1])
            reqc.cli020.success()

        except clusterlib.CLIError as exc:
                err_str = str(exc)
                if "MaxTxSizeUTxO" in err_str:
                    print(f"Fails at proposing {(num_pool_users)} constitution update actions in a single transaction")
                    return

    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_change_constitution_insufficient(
        self,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: tp.List[clusterlib.PoolUser],
    ):
        """Test enactment of change of constitution.

        * submit 3 "create constitution" actions
        * check that SPOs cannot vote on a "create constitution" action
        * 90 CC members and 100 DReps vote insufficiently to disapprove all constitution actions
        * check that the none of the actions are ratified or enacted
        """
        num_pool_users= 3
        pool_users_lg=pool_users_lg[:num_pool_users]
        total_participants = len(pool_users_lg)
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_lock_governance
        temp_template = common.get_test_id(cluster)

        # Create an action

        anchor_url = "http://www.const-action.com"

        constitution_file = f"{temp_template}_constitution.txt"
        constitution_text = (
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
            "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi "
            "ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit "
            "in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
            "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia "
            "deserunt mollit anim id est laborum."
        )
        with open(constitution_file, "w", encoding="utf-8") as out_fp:
            out_fp.write(constitution_text)

        constitution_url = "http://www.const-new.com"
        reqc.cli002.start(url=helpers.get_vcs_link())
        constitution_hash = cluster.g_conway_governance.get_anchor_data_hash(
            file_text=constitution_file
        )
        initial_gov_state = cluster.g_conway_governance.query.gov_state()
        initial_constitution_anchor = cluster.g_conway_governance.query.constitution()["anchor"]
        initial_enact_prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.CONSTITUTION,
                gov_state=initial_gov_state,
            )
        reqc.cli002.success()
        try:
            if conway_common.is_in_bootstrap(cluster_obj=cluster):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common.propose_change_constitution(
                        cluster_obj=cluster,
                        name_template=f"{temp_template}_constitution_bootstrap",
                        constitution_hash=constitution_hash,
                        pool_users=pool_users_lg,
                    )
                err_str = str(excinfo.value)
                assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
                return
            print(f"\n{total_participants} proposals for update constitution action are being submitted in a single transaction")
            _url = helpers.get_vcs_link()
            [r.start(url=_url) for r in (reqc.cli013, reqc.cip031a_02, reqc.cip031c_01, reqc.cip054_03)]
            (
                _,
                action_txid,
                action_ixs,
            ) = conway_common.propose_change_constitution(
                cluster_obj=cluster,
                name_template=f"{temp_template}_constitution",
                constitution_hash=constitution_hash,
                pool_users=pool_users_lg,
            )
            [r.success() for r in (reqc.cli013, reqc.cip031a_02, reqc.cip031c_01, reqc.cip054_03)]
            for action_ix in action_ixs:
                # Check that SPOs cannot vote on change of constitution action
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_with_spos",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_spo=True,
                    )
                err_str = str(excinfo.value)
                assert "StakePoolVoter" in err_str, err_str

            for action_ix in action_ixs:
                # Vote & disapprove the action
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.INSUFFICIENT,
                        vote_cc=True,
                        vote_drep=True,
                )

            print(len(governance_data.cc_members), " CC members are voting")
            print(len(governance_data.dreps_reg), " DReps are voting")
            
            ## disapprove action 1 and 2 
            ## approve action 3
            actions = [
                {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 1, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 2, "vote": conway_common.Votes.INSUFFICIENT},
            ]

            # Vote & approve the action
            reqc.cip042.start(url=helpers.get_vcs_link())
            for action in actions:
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_cc=True,
                    vote_drep=True,
                )

            def _check_cli_query():
                anchor = cluster.g_conway_governance.query.constitution()["anchor"]
                assert anchor == initial_constitution_anchor, "Constitution Anchor Changed."

            # Check ratification
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            rat_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
            ) 
            
            for action_ix in action_ixs:
                rat_action = governance_utils.lookup_ratified_actions(
                    gov_state=rat_gov_state, action_txid=action_txid, action_ix=action_ix
                )
                assert not rat_action, f"Action {action_txid}#{action_ix} ratified with insufficient votes."

            # Check enactment
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            )
            
            enact_prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.CONSTITUTION,
                gov_state=enact_gov_state,
            )
            # assert action_ix 2 in previous action
            assert enact_prev_action_rec.txid == initial_enact_prev_action_rec.txid, "Incorrect previous action Txid"
            assert enact_prev_action_rec.ix == initial_enact_prev_action_rec.ix, "Incorrect previous action index"
            
            [r.success() for r in (reqc.cip042, reqc.cip072, reqc.cip073_04)]

            reqc.cli036.start(url=helpers.get_vcs_link())
            _check_cli_query()
            reqc.cli036.success()

        except clusterlib.CLIError as exc:
                err_str = str(exc)
                if "MaxTxSizeUTxO" in err_str:
                    print(f"Fails at proposing {(num_pool_users)} constitution update actions in a single transaction")
                    return