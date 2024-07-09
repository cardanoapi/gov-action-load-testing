"""Tests for Conway governance state of no confidence."""

# pylint: disable=expression-not-assigned
from itertools import chain
import logging
import typing as tp

import allure
import pytest
from cardano_clusterlib import clusterlib

from cardano_node_tests.cluster_management import cluster_management
from cardano_node_tests.tests import common
from cardano_node_tests.tests import issues
from cardano_node_tests.tests import reqs_conway as reqc
from cardano_node_tests.tests.tests_conway import conway_common
from cardano_node_tests.utils import clusterlib_utils
from cardano_node_tests.utils import configuration
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


class TestNoConfidence:
    """Tests for state of no confidence."""

    @allure.link(helpers.get_vcs_link())
    @pytest.mark.skipif(not configuration.HAS_CC, reason="Runs only on setup with CC")
    @pytest.mark.long
    @pytest.mark.load_test
    def test_no_confidence_action_majority(
        self,
        cluster_manager: cluster_management.ClusterManager,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: clusterlib.PoolUser,
    ):
        """Test no confidence action.

        * create 3 "no confidence" actions
        * check that CC members votes have no effect
        * 3 SPOs and 100 DReps vote insufficiently to disapprove actions 1 and 2
        * 3 SPOs and 100 DReps vote majority to approve action 3
        * check that action 3 is enacted
        * check that it's not possible to vote on enacted action
        """
        try: 
            # pylint: disable=too-many-locals,too-many-statements
            num_pool_users = 3
            cluster, governance_data = cluster_lock_governance
            temp_template = common.get_test_id(cluster)
            pool_users_lg = pool_users_lg[:num_pool_users]
            total_participants = len(pool_users_lg)
            
            if conway_common.is_in_bootstrap(cluster_obj=cluster):
                pytest.skip("We can't create a needed 'update committee' previous action in bootstrap.")

            # Reinstate CC members first, if needed, so we have a previous action
            prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                gov_state=cluster.g_conway_governance.query.gov_state(),
            )
            if not prev_action_rec.txid:
                governance_setup.reinstate_committee(
                    cluster_obj=cluster,
                    governance_data=governance_data,
                    name_template=f"{temp_template}_reinstate1",
                    pool_user=pool_users_lg[0],
                )
                prev_action_rec = governance_utils.get_prev_action(
                    action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                    gov_state=cluster.g_conway_governance.query.gov_state(),
                )
            assert prev_action_rec.txid, "No previous action found, it is needed for 'no confidence'"
            
            init_return_account_balance = [cluster.g_query.get_stake_addr_info(
            pool_user_lg.stake.address
            ).reward_account_balance for pool_user_lg in pool_users_lg ]
            # Create an action
            deposit_amt = cluster.conway_genesis["govActionDeposit"]
            anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"

            _url = helpers.get_vcs_link()
            [
                r.start(url=_url)
                for r in (
                    reqc.cli018,
                    reqc.cip013,
                    reqc.cip029,
                    reqc.cip030en,
                    reqc.cip031a_04,
                    reqc.cip054_04,
                )
            ]
            no_confidence_actions = [
                cluster.g_conway_governance.action.create_no_confidence(
                    action_name=f"{temp_template}_{i}",
                    deposit_amt=deposit_amt,
                    anchor_url=f"http://www.cc-no-confidence{i}.com",
                    anchor_data_hash=anchor_data_hash,
                    prev_action_txid=prev_action_rec.txid,
                    prev_action_ix=prev_action_rec.ix,
                    deposit_return_stake_vkey_file=pool_users_lg[i].stake.vkey_file,
                )
                for i in range(total_participants)
            ]
            [r.success() for r in (reqc.cli018, reqc.cip031a_04, reqc.cip054_04)]
            actions_num = len(no_confidence_actions)
            
            print(f"\n{actions_num} proposals for no-confidence action are being submitted in a single transaction")
            
            tx_files_action = clusterlib.TxFiles(
                proposal_files=[no_confidence_action.action_file for no_confidence_action in no_confidence_actions],
                signing_key_files=[pool_user_lg.payment.skey_file for pool_user_lg in pool_users_lg],
            )

            # Make sure we have enough time to submit the proposal in one epoch
            clusterlib_utils.wait_for_epoch_interval(
                cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
            )
            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_users_lg[0].payment.address,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )
            
            out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
            combined_deposit_amt = deposit_amt * (actions_num)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_users_lg[0].payment.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_action.txins)
                - tx_output_action.fee
                - combined_deposit_amt
            ), f"Incorrect balance for source address `{pool_users_lg[0].payment.address}`"

            action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output_action.out_file)
            action_gov_state = cluster.g_conway_governance.query.gov_state()
            _cur_epoch = cluster.g_query.get_epoch()
            conway_common.save_gov_state(
                gov_state=action_gov_state, name_template=f"{temp_template}_action_{_cur_epoch}"
            )
            for action_ix in range(actions_num):
                prop_action = governance_utils.lookup_proposal(
                    gov_state=action_gov_state, action_txid=action_txid, action_ix=action_ix
                )
                assert prop_action, "No confidence action not found"
                assert (
                    prop_action["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.NO_CONFIDENCE.value
                ), "Incorrect action tag"

            reqc.cip029.success()

            # Check that CC cannot vote on "no confidence" action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_cc=True,
                    )
                err_str = str(excinfo.value)
                assert "CommitteeVoter" in err_str, err_str

            print(len(governance_data.dreps_reg), " DReps are voting")
            print(len(governance_data.pools_cold), " SPOs are voting")
            reqc.cip069en.start(url=helpers.get_vcs_link())
            
            enact_deposits_returned = [cluster.g_query.get_stake_addr_info(
                pool_user_lg.stake.address
                ).reward_account_balance for pool_user_lg in pool_users_lg ]
            print("\nenact_deposits_returned after cc votes disapproval: ", enact_deposits_returned)
            
            actions = [
                {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 1, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 2, "vote": conway_common.Votes.MAJORITY},
            ]
            for action in actions: 
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_drep=True,
                    vote_spo=True,
                )
            
            # wait an epoch for actions to be ratified 
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            rat_gov_state = cluster.g_conway_governance.query.gov_state()
            rat_action = governance_utils.lookup_ratified_actions(
                gov_state=rat_gov_state, action_txid=action_txid, action_ix=2
            )
            assert rat_action, f"Action {action_txid}#2 not ratified"   
            
            # Testnet will be in state of no confidence, respin is needed
            with cluster_manager.respin_on_failure():
                # Check enactment
                _url = helpers.get_vcs_link()
                [r.start(url=_url) for r in (reqc.cip032en, reqc.cip057)]
                
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
                enact_gov_state = cluster.g_conway_governance.query.gov_state()
                conway_common.save_gov_state(
                    gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
                )
                assert not conway_common.get_committee_val(
                    data=enact_gov_state
                ), "Committee is not empty"
                [r.success() for r in (reqc.cip013, reqc.cip039, reqc.cip057)]

                enact_prev_action_rec = governance_utils.get_prev_action(
                    action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                    gov_state=enact_gov_state,
                )
                
                assert enact_prev_action_rec.txid == action_txid, "Incorrect previous action Txid"
                assert enact_prev_action_rec.ix == 2, "Incorrect previous action index"
                
                [r.success() for r in (reqc.cip032en, reqc.cip069en)]

                reqc.cip034en.start(url=helpers.get_vcs_link())

                for i in range(total_participants):
                    enact_deposit_returned = cluster.g_query.get_stake_addr_info(
                        pool_users_lg[i].stake.address
                    ).reward_account_balance
                    
                    assert (
                        enact_deposit_returned == init_return_account_balance[i] + deposit_amt
                    ), "Incorrect return account balance"
                    [r.success() for r in (reqc.cip030en, reqc.cip034en)]

                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common.cast_vote(
                        cluster_obj=cluster,
                        governance_data=governance_data,
                        name_template=f"{temp_template}_enacted",
                        payment_addr=pool_users_lg[0].payment,
                        action_txid=action_txid,
                        action_ix=2, 
                        approve_drep=False,
                        approve_spo=False,
                    )
                err_str = str(excinfo.value)
                assert "(GovActionsDoNotExist" in err_str, err_str

                reqc.cip041.start(url=helpers.get_vcs_link())
                governance_setup.reinstate_committee(
                    cluster_obj=cluster,
                    governance_data=governance_data,
                    name_template=f"{temp_template}_reinstate2",
                    pool_user=pool_users_lg[0],
                )
                reqc.cip041.success()

            # Check action view
            governance_utils.check_action_view(cluster_obj=cluster, action_data=no_confidence_actions[2])

        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {num_pool_users} no-confidence actions in a single transaction")
                return

    @allure.link(helpers.get_vcs_link())
    @pytest.mark.skipif(not configuration.HAS_CC, reason="Runs only on setup with CC")
    @pytest.mark.long
    @pytest.mark.load_test
    def test_no_confidence_action_equal(
        self,
        cluster_manager: cluster_management.ClusterManager,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: clusterlib.PoolUser,
    ):
        """Test no confidence action.

        * create 3 "no confidence" actions
        * check that CC members votes have no effect
        * 3 SPOs and 100 DReps vote insufficiently to disapprove action 1
        * 3 SPOs and 100 DReps vote majority equally on actions 2 and 3
        * check that the first action on the proposal list with enough votes is enacted
        * check return account balance
        * check that it's not possible to vote on enacted action
        """
        try: 
            # pylint: disable=too-many-locals,too-many-statements
            num_pool_users = 3
            cluster, governance_data = cluster_lock_governance
            temp_template = common.get_test_id(cluster)
            pool_users_lg = pool_users_lg[:num_pool_users]
            total_participants = len(pool_users_lg)

            if conway_common.is_in_bootstrap(cluster_obj=cluster):
                pytest.skip("We can't create a needed 'update committee' previous action in bootstrap.")

            # Reinstate CC members first, if needed, so we have a previous action
            prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                gov_state=cluster.g_conway_governance.query.gov_state(),
            )
            if not prev_action_rec.txid:
                governance_setup.reinstate_committee(
                    cluster_obj=cluster,
                    governance_data=governance_data,
                    name_template=f"{temp_template}_reinstate1",
                    pool_user=pool_users_lg[0],
                )
                prev_action_rec = governance_utils.get_prev_action(
                    action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                    gov_state=cluster.g_conway_governance.query.gov_state(),
                )
            assert prev_action_rec.txid, "No previous action found, it is needed for 'no confidence'"

            init_return_account_balance = [cluster.g_query.get_stake_addr_info(
            pool_user_lg.stake.address
            ).reward_account_balance for pool_user_lg in pool_users_lg ]
            
            # Create an action
            deposit_amt = cluster.conway_genesis["govActionDeposit"]
            anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"

            _url = helpers.get_vcs_link()
            [
                r.start(url=_url)
                for r in (
                    reqc.cli018,
                    reqc.cip013,
                    reqc.cip029,
                    reqc.cip030en,
                    reqc.cip031a_04,
                    reqc.cip054_04,
                )
            ]
            no_confidence_actions = [
                cluster.g_conway_governance.action.create_no_confidence(
                    action_name=f"{temp_template}_{i}",
                    deposit_amt=deposit_amt,
                    anchor_url=f"http://www.cc-no-confidence{i}.com",
                    anchor_data_hash=anchor_data_hash,
                    prev_action_txid=prev_action_rec.txid,
                    prev_action_ix=prev_action_rec.ix,
                    deposit_return_stake_vkey_file=pool_users_lg[i].stake.vkey_file,
                )
                for i in range(total_participants)
            ]
            [r.success() for r in (reqc.cli018, reqc.cip031a_04, reqc.cip054_04)]
            actions_num = len(no_confidence_actions)
            print(f"\n{actions_num} proposals for no-confidence action are being submitted in a single transaction")
            
            tx_files_action = clusterlib.TxFiles(
                proposal_files=[no_confidence_action.action_file for no_confidence_action in no_confidence_actions],
                signing_key_files=[pool_user_lg.payment.skey_file for pool_user_lg in pool_users_lg],
            )

            # Make sure we have enough time to submit the proposal in one epoch
            clusterlib_utils.wait_for_epoch_interval(
                cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
            )
            address_utxos = [cluster.g_query.get_utxo(pool_user_lg.payment.address) for pool_user_lg in pool_users_lg]
            flatenned_utxos = list(chain.from_iterable(address_utxos))
            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_users_lg[0].payment.address,
                txins=flatenned_utxos,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )

            out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
            combined_deposit_amt = deposit_amt * (actions_num)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_users_lg[0].payment.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_action.txins)
                - tx_output_action.fee
                - combined_deposit_amt
            ), f"Incorrect balance for source address `{pool_users_lg[0].payment.address}`"

            action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output_action.out_file)
            action_gov_state = cluster.g_conway_governance.query.gov_state()
            _cur_epoch = cluster.g_query.get_epoch()
            conway_common.save_gov_state(
                gov_state=action_gov_state, name_template=f"{temp_template}_action_{_cur_epoch}"
            )
            for action_ix in range(actions_num):
                prop_action = governance_utils.lookup_proposal(
                    gov_state=action_gov_state, action_txid=action_txid, action_ix=action_ix
                )
                assert prop_action, "No confidence action not found"
                assert (
                    prop_action["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.NO_CONFIDENCE.value
                ), "Incorrect action tag"

            reqc.cip029.success()

            # Check that CC cannot vote on "no confidence" action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_cc=True,
                    )
                err_str = str(excinfo.value)
                assert "CommitteeVoter" in err_str, err_str

            print(len(governance_data.dreps_reg), " DReps are voting")
            print(len(governance_data.pools_cold), " SPOs are voting")
            reqc.cip069en.start(url=helpers.get_vcs_link())
            
            ## disapprove action 1 
            ## approve action 2 and 3
            actions = [
                {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 1, "vote": conway_common.Votes.MAJORITY},
                {"action_ix": 2, "vote": conway_common.Votes.MAJORITY},
            ]
            for action in actions: 
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_drep=True,
                    vote_spo=True,
                )
           
            # wait until actions move from proposal to being enacted
            while True:
                gov_state = cluster.g_conway_governance.query.gov_state()
                proposal_action=governance_utils.lookup_proposal(gov_state=gov_state,action_txid=action_txid, action_ix= 1)
                if proposal_action:
                    continue
                else:
                    break    
        
            # Testnet will be in state of no confidence, respin is needed
            with cluster_manager.respin_on_failure():
                # Check enactment
                _url = helpers.get_vcs_link()
                [r.start(url=_url) for r in (reqc.cip032en, reqc.cip057)]
                
                # Check action_ix 1 enactment
                # Check enactment
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
                enact_gov_state = cluster.g_conway_governance.query.gov_state()
                conway_common.save_gov_state(
                    gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
                )
                assert not conway_common.get_committee_val(
                    data=enact_gov_state
                ), "Committee is not empty"
                [r.success() for r in (reqc.cip013, reqc.cip039, reqc.cip057)]

                enact_prev_action_rec = governance_utils.get_prev_action(
                    action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                    gov_state=enact_gov_state,
                )
                
                assert enact_prev_action_rec.txid == action_txid, "Incorrect previous action Txid"
                assert enact_prev_action_rec.ix == 1, "Incorrect previous action index"
                
                [r.success() for r in (reqc.cip032en, reqc.cip069en)]

                reqc.cip034en.start(url=helpers.get_vcs_link())

                for i in range(total_participants):
                    enact_deposit_returned = cluster.g_query.get_stake_addr_info(
                        pool_users_lg[i].stake.address
                    ).reward_account_balance
                    
                    assert (
                        enact_deposit_returned == init_return_account_balance[i] + deposit_amt
                    ), "Incorrect return account balance"
                    [r.success() for r in (reqc.cip030en, reqc.cip034en)]

                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common.cast_vote(
                        cluster_obj=cluster,
                        governance_data=governance_data,
                        name_template=f"{temp_template}_enacted",
                        payment_addr=pool_users_lg[0].payment,
                        action_txid=action_txid,
                        action_ix=1, 
                        approve_drep=False,
                        approve_spo=False,
                    )
                err_str = str(excinfo.value)
                assert "(GovActionsDoNotExist" in err_str, err_str

                reqc.cip041.start(url=helpers.get_vcs_link())
                governance_setup.reinstate_committee(
                    cluster_obj=cluster,
                    governance_data=governance_data,
                    name_template=f"{temp_template}_reinstate2",
                    pool_user=pool_users_lg[0],
                )
                reqc.cip041.success()

            # Check action view
            governance_utils.check_action_view(cluster_obj=cluster, action_data=no_confidence_actions[1])

        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {num_pool_users} no-confidence actions in a single transaction")
                return
    
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.skipif(not configuration.HAS_CC, reason="Runs only on setup with CC")
    @pytest.mark.long
    @pytest.mark.load_test
    def test_no_confidence_action_insufficient(
        self,
        cluster_manager: cluster_management.ClusterManager,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: clusterlib.PoolUser,
    ):
        """Test no confidence action.

        * create 3 "no confidence" actions
        * check that CC members votes have no effect
        * 3 SPOs and 100 DReps vote insufficiently to disapprove all no confidence actions
        * check that none of the actions are enacted
        * check return account balance
        """
        try: 
            # pylint: disable=too-many-locals,too-many-statements
            num_pool_users = 3
            cluster, governance_data = cluster_lock_governance
            temp_template = common.get_test_id(cluster)
            pool_users_lg = pool_users_lg[:num_pool_users]
            total_participants = len(pool_users_lg)

            if conway_common.is_in_bootstrap(cluster_obj=cluster):
                pytest.skip("We can't create a needed 'update committee' previous action in bootstrap.")

            # Reinstate CC members first, if needed, so we have a previous action
            prev_action_rec = governance_utils.get_prev_action(
                action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                gov_state=cluster.g_conway_governance.query.gov_state(),
            )
            if not prev_action_rec.txid:
                governance_setup.reinstate_committee(
                    cluster_obj=cluster,
                    governance_data=governance_data,
                    name_template=f"{temp_template}_reinstate1",
                    pool_user=pool_users_lg[0],
                )
                prev_action_rec = governance_utils.get_prev_action(
                    action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                    gov_state=cluster.g_conway_governance.query.gov_state(),
                )
            assert prev_action_rec.txid, "No previous action found, it is needed for 'no confidence'"

            init_return_account_balance = [cluster.g_query.get_stake_addr_info(
            pool_user_lg.stake.address
            ).reward_account_balance for pool_user_lg in pool_users_lg ]
            
            # Create an action
            deposit_amt = cluster.conway_genesis["govActionDeposit"]
            anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"

            _url = helpers.get_vcs_link()
            [
                r.start(url=_url)
                for r in (
                    reqc.cli018,
                    reqc.cip013,
                    reqc.cip029,
                    reqc.cip030en,
                    reqc.cip031a_04,
                    reqc.cip054_04,
                )
            ]
            no_confidence_actions = [
                cluster.g_conway_governance.action.create_no_confidence(
                    action_name=f"{temp_template}_{i}",
                    deposit_amt=deposit_amt,
                    anchor_url=f"http://www.cc-no-confidence{i}.com",
                    anchor_data_hash=anchor_data_hash,
                    prev_action_txid=prev_action_rec.txid,
                    prev_action_ix=prev_action_rec.ix,
                    deposit_return_stake_vkey_file=pool_users_lg[i].stake.vkey_file,
                )
                for i in range(total_participants)
            ]
            [r.success() for r in (reqc.cli018, reqc.cip031a_04, reqc.cip054_04)]
            actions_num = len(no_confidence_actions)
            print(f"\n{actions_num} proposals for no-confidence action are being submitted in a single transaction")
            
            tx_files_action = clusterlib.TxFiles(
                proposal_files=[no_confidence_action.action_file for no_confidence_action in no_confidence_actions],
                signing_key_files=[pool_user_lg.payment.skey_file for pool_user_lg in pool_users_lg],
            )

            # Make sure we have enough time to submit the proposal in one epoch
            clusterlib_utils.wait_for_epoch_interval(
                cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
            )
            address_utxos = [cluster.g_query.get_utxo(pool_user_lg.payment.address) for pool_user_lg in pool_users_lg]
            flatenned_utxos = list(chain.from_iterable(address_utxos))
            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_users_lg[0].payment.address,
                txins=flatenned_utxos,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )

            out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
            combined_deposit_amt = deposit_amt * (actions_num)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_users_lg[0].payment.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_action.txins)
                - tx_output_action.fee
                - combined_deposit_amt
            ), f"Incorrect balance for source address `{pool_users_lg[0].payment.address}`"

            action_txid = cluster.g_transaction.get_txid(tx_body_file=tx_output_action.out_file)
            action_gov_state = cluster.g_conway_governance.query.gov_state()
            _cur_epoch = cluster.g_query.get_epoch()
            conway_common.save_gov_state(
                gov_state=action_gov_state, name_template=f"{temp_template}_action_{_cur_epoch}"
            )
            for action_ix in range(actions_num):
                prop_action = governance_utils.lookup_proposal(
                    gov_state=action_gov_state, action_txid=action_txid, action_ix=action_ix
                )
                assert prop_action, "No confidence action not found"
                assert (
                    prop_action["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.NO_CONFIDENCE.value
                ), "Incorrect action tag"

            reqc.cip029.success()

            # Check that CC cannot vote on "no confidence" action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_lg[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_cc=True,
                    )
                err_str = str(excinfo.value)
                assert "CommitteeVoter" in err_str, err_str

            print(len(governance_data.dreps_reg), " DReps are voting")
            print(len(governance_data.pools_cold), " SPOs are voting")
            reqc.cip069en.start(url=helpers.get_vcs_link())
            
            ## disapprove all actions
            actions = [
                {"action_ix": 0, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 1, "vote": conway_common.Votes.INSUFFICIENT},
                {"action_ix": 2, "vote": conway_common.Votes.INSUFFICIENT},
            ]
            for action in actions: 
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=action["action_ix"],
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=action["vote"],
                    vote_drep=True,
                    vote_spo=True,
                )
           
            # wait until actions move from proposal to being expired
            while True:
                gov_state = cluster.g_conway_governance.query.gov_state()
                proposal_action=governance_utils.lookup_proposal(gov_state=gov_state,action_txid=action_txid, action_ix= 0)
                if proposal_action:
                    continue
                else:
                    break    
        
            # Testnet will be in state of no confidence, respin is needed
            with cluster_manager.respin_on_failure():
                # Check enactment
                _url = helpers.get_vcs_link()
                [r.start(url=_url) for r in (reqc.cip032en, reqc.cip057)]
                
                # Check no enactment
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
                enact_gov_state = cluster.g_conway_governance.query.gov_state()
                conway_common.save_gov_state(
                    gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
                )
                assert conway_common.get_committee_val(
                    data=enact_gov_state
                ), "Committee is empty"
                [r.success() for r in (reqc.cip013, reqc.cip039, reqc.cip057)]

                enact_prev_action_rec = governance_utils.get_prev_action(
                    action_type=governance_utils.PrevGovActionIds.COMMITTEE,
                    gov_state=enact_gov_state,
                )
                
                # check that previous action id and ix must not change
                assert enact_prev_action_rec.txid == prev_action_rec.txid, "Incorrect previous action Txid"
                assert enact_prev_action_rec.ix == prev_action_rec.ix, "Incorrect previous action index"
                
                [r.success() for r in (reqc.cip032en, reqc.cip069en)]

                reqc.cip034en.start(url=helpers.get_vcs_link())

                for i in range(total_participants):
                    enact_deposit_returned = cluster.g_query.get_stake_addr_info(
                        pool_users_lg[i].stake.address
                    ).reward_account_balance
                    
                    assert (
                        enact_deposit_returned == init_return_account_balance[i] + deposit_amt
                    ), "Incorrect return account balance"
                    [r.success() for r in (reqc.cip030en, reqc.cip034en)]

        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {num_pool_users} no-confidence actions in a single transaction")
                return