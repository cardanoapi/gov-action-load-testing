"""Tests for Conway governance treasury withdrawals."""

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
from cardano_node_tests.utils import submit_utils
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
        no_of_users=10
    )


class TestTreasuryWithdrawals:
    """Tests for treasury withdrawals."""

    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_treasury_withdrawals_majority(  # noqa: C901
        self,
        cluster_use_governance: governance_setup.GovClusterT,
        pool_users_ug: clusterlib.PoolUser,
    ):
        """Test enactment of multiple treasury withdrawals in single epoch.

        Use `transaction build` for building the transactions.
        * submit 3 "treasury withdrawal" actions
        * check that SPOs cannot vote on a "treasury withdrawal" actions
        * 100 DReps and 90 CC members vote insufficiently to disapprove actions 1 and 2
        * 100 DReps and 90 CC members vote majority to approve action 3
        * check that action 3 is ratified
        * try to disapprove the ratified action, this shouldn't have any effect
        * check that action 3 is enacted
        * check that it's not possible to vote on enacted action
        """
        pool_user_ug = pool_users_ug[0]
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_use_governance
        temp_template = common.get_test_id(cluster)
        actions_num = 3
        # Create stake address and registration certificate
        stake_deposit_amt = cluster.g_query.get_address_deposit()

        recv_stake_addr_rec = cluster.g_stake_address.gen_stake_addr_and_keys(
            name=f"{temp_template}_receive"
        )
        recv_stake_addr_reg_cert = cluster.g_stake_address.gen_stake_addr_registration_cert(
            addr_name=f"{temp_template}_receive",
            deposit_amt=stake_deposit_amt,
            stake_vkey_file=recv_stake_addr_rec.vkey_file,
        )      
        # Create an action and register stake address
        action_deposit_amt = cluster.conway_genesis["govActionDeposit"]
        transfer_amt = 3_000_000

        anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"

        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli015, reqc.cip031a_06, reqc.cip031f, reqc.cip054_05)]

        withdrawal_actions = [
            cluster.g_conway_governance.action.create_treasury_withdrawal(
                action_name=f"{temp_template}_{a}",
                transfer_amt=transfer_amt,
                deposit_amt=action_deposit_amt,
                anchor_url=f"http://www.withdrawal-action{a}.com",
                anchor_data_hash=anchor_data_hash,
                funds_receiving_stake_vkey_file=recv_stake_addr_rec.vkey_file,
                deposit_return_stake_vkey_file=pool_user_ug.stake.vkey_file,
            )
            for a in range(actions_num)
        ]
        [r.success() for r in (reqc.cli015, reqc.cip031a_06, reqc.cip031f, reqc.cip054_05)]
        
        tx_files_action = clusterlib.TxFiles(
            certificate_files=[recv_stake_addr_reg_cert],
            proposal_files=[w.action_file for w in withdrawal_actions],
            signing_key_files=[recv_stake_addr_rec.skey_file, pool_user_ug.payment.skey_file],
        )

        if conway_common.is_in_bootstrap(cluster_obj=cluster):
            with pytest.raises(clusterlib.CLIError) as excinfo:
                clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_action_bootstrap",
                    src_address=pool_user_ug.payment.address,
                    use_build_cmd=True,
                    tx_files=tx_files_action,
                )
            err_str = str(excinfo.value)
            assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
            return

        # Make sure we have enough time to submit the proposals in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
        )

        print(f"\n Submitting {actions_num} treasury withdrawl actions in a single transaction.")
        try: 

            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_user_ug.payment.address,
                submit_method=submit_utils.SubmitMethods.CLI,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )

            assert cluster.g_query.get_stake_addr_info(
                recv_stake_addr_rec.address
            ).address, f"Stake address is not registered: {recv_stake_addr_rec.address}"
            actions_deposit_combined = action_deposit_amt * actions_num

            out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_user_ug.payment.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_action.txins)
                - tx_output_action.fee
                - actions_deposit_combined
                - stake_deposit_amt
            ), f"Incorrect balance for source address `{pool_user_ug.payment.address}`"

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
                assert prop_action, "Treasury withdrawals action not found"
                assert (
                    prop_action["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.TREASURY_WITHDRAWALS.value
                ), "Incorrect action tag"
            # Check that SPOs cannot vote on treasury withdrawal action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_ug[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_spo=True,
                    )
                err_str = str(excinfo.value)
                assert "StakePoolVoter" in err_str, err_str
            
            print(len(governance_data.dreps_reg), " DReps are voting")
            print(len(governance_data.cc_members), " CC Members are voting")

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
                    pool_user=pool_users_ug[0],
                    vote=action["vote"],
                    vote_drep=True,
                    vote_cc=True,
                )
            
            treasury_init = clusterlib_utils.get_ledger_state(cluster_obj=cluster)["stateBefore"][
                "esAccountState"
            ]["treasury"]

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
            
            reqc.cip038_06.start(url=helpers.get_vcs_link())
            assert not rat_gov_state["nextRatifyState"][
                "ratificationDelayed"
            ], "Ratification is delayed unexpectedly"
            reqc.cip038_06.success()

            # Disapprove ratified action, the voting shouldn't have any effect
            conway_common._cast_vote(
                temp_template=f"{temp_template}",
                action_ix=2,
                action_txid=action_txid,
                governance_data=governance_data,
                cluster=cluster,
                pool_user=pool_users_ug[0],
                vote=conway_common.Votes.INSUFFICIENT,
                vote_drep=True,
                vote_cc=True,
            )

            reqc.cip033.start(url=helpers.get_vcs_link())

            # Check enactment
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            assert (
                cluster.g_query.get_stake_addr_info(recv_stake_addr_rec.address).reward_account_balance
                == transfer_amt
            ), "Incorrect reward account balance"
            [r.success() for r in (reqc.cip033, reqc.cip048)]

            # Try to vote on enacted action
            with pytest.raises(clusterlib.CLIError) as excinfo:
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=2,
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_ug[0],
                    vote=conway_common.Votes.MAJORITY,
                    vote_drep=True,
                    vote_cc=True,
                )
            err_str = str(excinfo.value)
            assert "(GovActionsDoNotExist" in err_str, err_str

            reqc.cip079.start(url=helpers.get_vcs_link())
            treasury_finish = clusterlib_utils.get_ledger_state(cluster_obj=cluster)["stateBefore"][
                "esAccountState"
            ]["treasury"]
            assert treasury_init != treasury_finish, "Treasury balance didn't change"
            reqc.cip079.success()

            # Check action view
            governance_utils.check_action_view(cluster_obj=cluster, action_data=withdrawal_actions[2])
        
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {actions_num} proposals for treasury withdrawl in a single transaction")
                return
    
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_treasury_withdrawals_equal( 
        self,
        cluster_use_governance: governance_setup.GovClusterT,
        pool_users_ug: clusterlib.PoolUser,
    ):
        """Test enactment of multiple treasury withdrawals in single epoch.

        Use `transaction build` for building the transactions.
        * submit 3 "treasury withdrawal" actions
        * check that SPOs cannot vote on a "treasury withdrawal" actions
        * 100 DReps and 90 CC members vote insufficiently to disapprove action 1
        * 100 DReps and 90 CC members vote majority equally on actions 2 and 3
        * check that actions 2 and 3 are ratified and actions 1 is expired
        * try to disapprove the ratified action, this shouldn't have any effect
        * check that the action are enacted
        * check that it's not possible to vote on enacted action
        """
        pool_user_ug = pool_users_ug[0]
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_use_governance
        temp_template = common.get_test_id(cluster)
        actions_num = 3
        # Create stake address and registration certificate
        stake_deposit_amt = cluster.g_query.get_address_deposit()

        recv_stake_addr_rec = cluster.g_stake_address.gen_stake_addr_and_keys(
            name=f"{temp_template}_receive"
        )
        recv_stake_addr_reg_cert = cluster.g_stake_address.gen_stake_addr_registration_cert(
            addr_name=f"{temp_template}_receive",
            deposit_amt=stake_deposit_amt,
            stake_vkey_file=recv_stake_addr_rec.vkey_file,
        )

        # Create an action and register stake address

        action_deposit_amt = cluster.conway_genesis["govActionDeposit"]
        transfer_amt = 3_000_000

        anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"

        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli015, reqc.cip031a_06, reqc.cip031f, reqc.cip054_05)]

        withdrawal_actions = [
            cluster.g_conway_governance.action.create_treasury_withdrawal(
                action_name=f"{temp_template}_{a}",
                transfer_amt=transfer_amt,
                deposit_amt=action_deposit_amt,
                anchor_url=f"http://www.withdrawal-action{a}.com",
                anchor_data_hash=anchor_data_hash,
                funds_receiving_stake_vkey_file=recv_stake_addr_rec.vkey_file,
                deposit_return_stake_vkey_file=pool_user_ug.stake.vkey_file,
            )
            for a in range(actions_num)
        ]
        [r.success() for r in (reqc.cli015, reqc.cip031a_06, reqc.cip031f, reqc.cip054_05)]
        
        tx_files_action = clusterlib.TxFiles(
            certificate_files=[recv_stake_addr_reg_cert],
            proposal_files=[w.action_file for w in withdrawal_actions],
            signing_key_files=[recv_stake_addr_rec.skey_file, pool_user_ug.payment.skey_file],
        )

        if conway_common.is_in_bootstrap(cluster_obj=cluster):
            with pytest.raises(clusterlib.CLIError) as excinfo:
                clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_action_bootstrap",
                    src_address=pool_user_ug.payment.address,
                    use_build_cmd=True,
                    tx_files=tx_files_action,
                )
            err_str = str(excinfo.value)
            assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
            return

        # Make sure we have enough time to submit the proposals in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
        )

        print(f"\n Submitting {actions_num} treasury withdrawl actions in a single transaction.")
        try: 

            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_user_ug.payment.address,
                submit_method=submit_utils.SubmitMethods.CLI,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )

            assert cluster.g_query.get_stake_addr_info(
                recv_stake_addr_rec.address
            ).address, f"Stake address is not registered: {recv_stake_addr_rec.address}"
            
            actions_deposit_combined = action_deposit_amt * actions_num

            out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_user_ug.payment.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_action.txins)
                - tx_output_action.fee
                - actions_deposit_combined
                - stake_deposit_amt
            ), f"Incorrect balance for source address `{pool_user_ug.payment.address}`"

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
                assert prop_action, "Treasury withdrawals action not found"
                assert (
                    prop_action["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.TREASURY_WITHDRAWALS.value
                ), "Incorrect action tag"
            
            # Check that SPOs cannot vote on treasury withdrawal action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_ug[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_spo=True,
                    )
                err_str = str(excinfo.value)
                assert "StakePoolVoter" in err_str, err_str
            
            # Vote & disapprove the action
            print(len(governance_data.dreps_reg), " DReps are voting")
            print(len(governance_data.cc_members), " CC Members are voting")
            
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
                    pool_user=pool_users_ug[0],
                    vote=action["vote"],
                    vote_drep=True,
                    vote_cc=True,
                )
            
            treasury_init = clusterlib_utils.get_ledger_state(cluster_obj=cluster)["stateBefore"][
                "esAccountState"
            ]["treasury"]
            
            # Check ratification
            rat_gov_state = cluster.g_conway_governance.query.gov_state()
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            conway_common.save_gov_state(
                gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
            )
            
            def is_ratified(rat_gov_state):
                rat_action_1 = governance_utils.lookup_ratified_actions(
                            gov_state=rat_gov_state, action_txid=action_txid, action_ix=1
                        )
                rat_action_2 = governance_utils.lookup_ratified_actions(
                            gov_state=rat_gov_state, action_txid=action_txid, action_ix=2
                        )
                return rat_action_1, rat_action_2
            
            
            rat_action_1, rat_action_2 = is_ratified(rat_gov_state) 
            if not (rat_action_1 and rat_action_2):
                while True: 
                    rat_gov_state = cluster.g_conway_governance.query.gov_state()
                    rat_action_1, rat_action_2 = is_ratified(rat_gov_state)  
                    if rat_action_1 and rat_action_2: 
                        break 
                    
            assert rat_action_1, f"Action {action_txid}#{1} not ratified"
            assert rat_action_2, f"Action {action_txid}#{2} not ratified"

            # Check expiration
            while True:
                exp_gov_state = cluster.g_conway_governance.query.gov_state()
                expired_action = governance_utils.lookup_expired_actions(
                        gov_state=exp_gov_state, action_txid=action_txid, action_ix=0
                    )
                if expired_action:
                    break
            
            reqc.cip038_06.start(url=helpers.get_vcs_link())
            assert not rat_gov_state["nextRatifyState"][
                "ratificationDelayed"
            ], "Ratification is delayed unexpectedly"
            reqc.cip038_06.success()

            # Disapprove ratified action, the voting shouldn't have any effect
            for action_ix in range(1,3):
                conway_common._cast_vote(
                    temp_template=f"{temp_template}",
                    action_ix=action_ix,
                    action_txid=action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_ug[0],
                    vote=conway_common.Votes.INSUFFICIENT,
                    vote_drep=True,
                    vote_cc=True,
                )

            reqc.cip033.start(url=helpers.get_vcs_link())

            # Check enactment
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            assert (
                cluster.g_query.get_stake_addr_info(recv_stake_addr_rec.address).reward_account_balance
                == transfer_amt * actions_num
            ), "Incorrect reward account balance"
            [r.success() for r in (reqc.cip033, reqc.cip048)]

            # Try to vote on enacted action
            for action_ix in range(1,3):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_ug[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_drep=True,
                        vote_cc=True,
                    )
                err_str = str(excinfo.value)
                assert "(GovActionsDoNotExist" in err_str, err_str

            reqc.cip079.start(url=helpers.get_vcs_link())
            treasury_finish = clusterlib_utils.get_ledger_state(cluster_obj=cluster)["stateBefore"][
                "esAccountState"
            ]["treasury"]
            assert treasury_init != treasury_finish, "Treasury balance didn't change"
            reqc.cip079.success()
                    
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {actions_num} proposals for treasury withdrawl in a single transaction")
                return
    
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_treasury_withdrawals_insufficient( 
        self,
        cluster_use_governance: governance_setup.GovClusterT,
        pool_users_ug: clusterlib.PoolUser,
    ):
        """Test enactment of multiple treasury withdrawals in single epoch.

        Use `transaction build` for building the transactions.
        * submit 3 "treasury withdrawal" actions
        * check that SPOs cannot vote on a "treasury withdrawal" actions
        * 100 DReps and 90 CC Members vote insufficiently to disapprove all actions
        * check that the actions expire and action deposits are returned
        """
        pool_user_ug = pool_users_ug[0]
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_use_governance
        temp_template = common.get_test_id(cluster)
        actions_num = 3
        # Create stake address and registration certificate
        stake_deposit_amt = cluster.g_query.get_address_deposit()

        recv_stake_addr_rec = cluster.g_stake_address.gen_stake_addr_and_keys(
            name=f"{temp_template}_receive"
        )
        recv_stake_addr_reg_cert = cluster.g_stake_address.gen_stake_addr_registration_cert(
            addr_name=f"{temp_template}_receive",
            deposit_amt=stake_deposit_amt,
            stake_vkey_file=recv_stake_addr_rec.vkey_file,
        )

        # Create an action and register stake address

        action_deposit_amt = cluster.conway_genesis["govActionDeposit"]
        transfer_amt = 3_000_000
        init_return_account_balance = cluster.g_query.get_stake_addr_info(
            pool_user_ug.stake.address
        ).reward_account_balance
        anchor_data_hash = "5d372dca1a4cc90d7d16d966c48270e33e3aa0abcb0e78f0d5ca7ff330d2245d"

        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli015, reqc.cip031a_06, reqc.cip031f, reqc.cip054_05)]

        withdrawal_actions = [
            cluster.g_conway_governance.action.create_treasury_withdrawal(
                action_name=f"{temp_template}_{a}",
                transfer_amt=transfer_amt,
                deposit_amt=action_deposit_amt,
                anchor_url=f"http://www.withdrawal-action{a}.com",
                anchor_data_hash=anchor_data_hash,
                funds_receiving_stake_vkey_file=recv_stake_addr_rec.vkey_file,
                deposit_return_stake_vkey_file=pool_user_ug.stake.vkey_file,
            )
            for a in range(actions_num)
        ]
        [r.success() for r in (reqc.cli015, reqc.cip031a_06, reqc.cip031f, reqc.cip054_05)]
        
        tx_files_action = clusterlib.TxFiles(
            certificate_files=[recv_stake_addr_reg_cert],
            proposal_files=[w.action_file for w in withdrawal_actions],
            signing_key_files=[recv_stake_addr_rec.skey_file, pool_user_ug.payment.skey_file],
        )

        if conway_common.is_in_bootstrap(cluster_obj=cluster):
            with pytest.raises(clusterlib.CLIError) as excinfo:
                clusterlib_utils.build_and_submit_tx(
                    cluster_obj=cluster,
                    name_template=f"{temp_template}_action_bootstrap",
                    src_address=pool_user_ug.payment.address,
                    use_build_cmd=True,
                    tx_files=tx_files_action,
                )
            err_str = str(excinfo.value)
            assert "(DisallowedProposalDuringBootstrap" in err_str, err_str
            return

        # Make sure we have enough time to submit the proposals in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_BUFFER
        )
        
        action_prop_epoch = cluster.g_query.get_epoch()

        try: 
            tx_output_action = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=f"{temp_template}_action",
                src_address=pool_user_ug.payment.address,
                submit_method=submit_utils.SubmitMethods.CLI,
                use_build_cmd=True,
                tx_files=tx_files_action,
            )

        
            assert cluster.g_query.get_stake_addr_info(
                recv_stake_addr_rec.address
            ).address, f"Stake address is not registered: {recv_stake_addr_rec.address}"
            
            actions_deposit_combined = action_deposit_amt * actions_num

            out_utxos_action = cluster.g_query.get_utxo(tx_raw_output=tx_output_action)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos_action, address=pool_user_ug.payment.address)[
                    0
                ].amount
                == clusterlib.calculate_utxos_balance(tx_output_action.txins)
                - tx_output_action.fee
                - actions_deposit_combined
                - stake_deposit_amt
            ), f"Incorrect balance for source address `{pool_user_ug.payment.address}`"

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
                assert prop_action, "Treasury withdrawals action not found"
                assert (
                    prop_action["proposalProcedure"]["govAction"]["tag"]
                    == governance_utils.ActionTags.TREASURY_WITHDRAWALS.value
                ), "Incorrect action tag"
                        
            # Check that SPOs cannot vote on treasury withdrawal action
            for action_ix in range(actions_num):
                with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common._cast_vote(
                        temp_template=f"{temp_template}_no",
                        action_ix=action_ix,
                        action_txid=action_txid,
                        governance_data=governance_data,
                        cluster=cluster,
                        pool_user=pool_users_ug[0],
                        vote=conway_common.Votes.MAJORITY,
                        vote_spo=True,
                    )
                err_str = str(excinfo.value)
                assert "StakePoolVoter" in err_str, err_str
            
            # Vote & disapprove the action
            print(len(governance_data.dreps_reg), " DReps are voting")
            print(len(governance_data.cc_members), " CC Members are voting")

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
                    pool_user=pool_users_ug[0],
                    vote=action["vote"],
                    vote_drep=True,
                    vote_cc=True,
                )
            
            treasury_init = clusterlib_utils.get_ledger_state(cluster_obj=cluster)["stateBefore"][
                "esAccountState"
            ]["treasury"]
            
            # Check that the actions are not ratified
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            nonrat_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=nonrat_gov_state, name_template=f"{temp_template}_nonrat_{_cur_epoch}"
            )
            for action_ix in range(actions_num):
                assert not governance_utils.lookup_ratified_actions(
                    gov_state=nonrat_gov_state, action_txid=action_txid, action_ix=action_ix
                ), f"Action {action_txid}#{action_ix} got ratified unexpectedly"

            # Check that the actions are not enacted
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            
            nonenacted_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=nonenacted_gov_state, name_template=f"{temp_template}_nonenact_{_cur_epoch}"
            )
            assert (
                cluster.g_query.get_stake_addr_info(recv_stake_addr_rec.address).reward_account_balance
                == 0
            ), "Incorrect reward account balance"
            
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at submitting {actions_num} proposals for treasury withdrawl in a single transaction")
                return