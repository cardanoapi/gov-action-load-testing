"""Tests for Conway governance protocol parameters update."""

# pylint: disable=expression-not-assigned
import fractions
import logging
import pathlib as pl
import random
import typing as tp

import allure
import pytest
from cardano_clusterlib import clusterlib

from cardano_node_tests.cluster_management import cluster_management
from cardano_node_tests.tests import common
from cardano_node_tests.tests import reqs_conway as reqc
from cardano_node_tests.tests.tests_conway import conway_common
from cardano_node_tests.utils import clusterlib_utils
from cardano_node_tests.utils import configuration
from cardano_node_tests.utils import governance_setup
from cardano_node_tests.utils import governance_utils
from cardano_node_tests.utils import helpers
from cardano_node_tests.utils.versions import VERSIONS

LOGGER = logging.getLogger(__name__)
DATA_DIR = pl.Path(__file__).parent.parent / "data"

pytestmark = pytest.mark.skipif(
    VERSIONS.transaction_era < VERSIONS.CONWAY,
    reason="runs only with Tx era >= Conway",
)


NETWORK_GROUP_PPARAMS = {
    "maxBlockBodySize",
    "maxTxSize",
    "maxBlockHeaderSize",
    "maxValueSize",
    "maxTxExecutionUnits",
    "maxBlockExecutionUnits",
    "maxCollateralInputs",
}

ECONOMIC_GROUP_PPARAMS = {
    "txFeePerByte",
    "txFeeFixed",
    "stakeAddressDeposit",
    "stakePoolDeposit",
    "monetaryExpansion",
    "treasuryCut",
    "minPoolCost",
    "utxoCostPerByte",
    "executionUnitPrices",
}

TECHNICAL_GROUP_PPARAMS = {
    "poolPledgeInfluence",
    "poolRetireMaxEpoch",
    "stakePoolTargetNum",
    "costModels",
    "collateralPercentage",
}

GOVERNANCE_GROUP_PPARAMS = {
    "govActionLifetime",
    "govActionDeposit",
    "dRepDeposit",
    "dRepActivity",
    "committeeMinSize",
    "committeeMaxTermLength",
}

GOVERNANCE_GROUP_PPARAMS_DREP_THRESHOLDS = {
    "committeeNoConfidence",
    "committeeNormal",
    "hardForkInitiation",
    "motionNoConfidence",
    "ppEconomicGroup",
    "ppGovGroup",
    "ppNetworkGroup",
    "ppTechnicalGroup",
    "treasuryWithdrawal",
    "updateToConstitution",
}

GOVERNANCE_GROUP_PPARAMS_POOL_THRESHOLDS = {
    "committeeNoConfidence",
    "committeeNormal",
    "hardForkInitiation",
    "motionNoConfidence",
    "ppSecurityGroup",
}

# Security related pparams that require also SPO approval
SECURITY_PPARAMS = {
    "maxBlockBodySize",
    "maxTxSize",
    "maxBlockHeaderSize",
    "maxValueSize",
    "maxBlockExecutionUnits",
    "txFeePerByte",
    "txFeeFixed",
    "utxoCostPerByte",
    "govActionDeposit",
    "minFeeRefScriptsCoinsPerByte",  # not in 8.8 release yet
}


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
        fund_amount=2000_000_000,
        no_of_users=300
    )


def _get_rational_str(value: float) -> str:
    return str(fractions.Fraction(value).limit_denominator())


def _check_w_denominator(
    update_proposal: clusterlib_utils.UpdateProposal, pparam: tp.Union[float, dict]
) -> bool:
    exp_val: tp.Union[float, dict, str] = pparam
    if isinstance(pparam, dict):
        exp_val = f"{pparam['numerator']}/{pparam['denominator']}"
    return bool(update_proposal.value == exp_val)


def _check_max_tx_execution_units(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    pparam = protocol_params["maxTxExecutionUnits"]
    exp_val = f"({pparam['steps']},{pparam['memory']})"
    return bool(update_proposal.value == exp_val)


def _check_max_block_execution_units(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    pparam = protocol_params["maxBlockExecutionUnits"]
    exp_val = f"({pparam['steps']},{pparam['memory']})"
    return bool(update_proposal.value == exp_val)


def _check_execution_unit_prices_mem(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    return _check_w_denominator(
        update_proposal=update_proposal,
        pparam=protocol_params["executionUnitPrices"]["priceMemory"],
    )


def _check_execution_unit_prices_steps(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    return _check_w_denominator(
        update_proposal=update_proposal, pparam=protocol_params["executionUnitPrices"]["priceSteps"]
    )


def _check_monetary_expansion(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    return _check_w_denominator(
        update_proposal=update_proposal, pparam=protocol_params["monetaryExpansion"]
    )


def _check_treasury_expansion(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    return _check_w_denominator(
        update_proposal=update_proposal, pparam=protocol_params["treasuryCut"]
    )


def _check_pool_pledge_influence(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    return _check_w_denominator(
        update_proposal=update_proposal, pparam=protocol_params["poolPledgeInfluence"]
    )


def _check_pool_thresholds(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    return _check_w_denominator(
        update_proposal=update_proposal,
        pparam=protocol_params["poolVotingThresholds"][update_proposal.name],
    )


def _check_drep_thresholds(
    update_proposal: clusterlib_utils.UpdateProposal, protocol_params: dict
) -> bool:
    return _check_w_denominator(
        update_proposal=update_proposal,
        pparam=protocol_params["dRepVotingThresholds"][update_proposal.name],
    )


class TestPParamUpdate:
    """Tests for protocol parameters update."""
    
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_pparam_update_majority(
        self,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: tp.List[clusterlib.PoolUser], 
    ):
        """Test protocol parameter update.

        * submit 3 "protocol parameters update" action
        * 3 SPOs, 90 CC Members and 100 DReps vote insufficiently to disapprove actions 1 and 2 
        * 3 SPOs, 90 CC Members and 100 DReps vote majority to approve action 3
        * check that action 3 is ratified
        * check that action 3 is enacted 
        """
        
        num_pool_users = 3
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_lock_governance
        temp_template = common.get_test_id(cluster)
        cost_proposal_file = DATA_DIR / "cost_models_list.json"
        is_in_bootstrap = conway_common.is_in_bootstrap(cluster_obj=cluster)

        if is_in_bootstrap and not configuration.HAS_CC:
            pytest.skip("The test doesn't work in bootstrap period without CC.")

        # Check if total delegated stake is below the threshold. This can be used to check that
        # undelegated stake is treated as Abstain. If undelegated stake was treated as Yes, than
        # missing votes would approve the action.
        delegated_stake = governance_utils.get_delegated_stake(cluster_obj=cluster)
        cur_pparams = cluster.g_conway_governance.query.gov_state()["currentPParams"]
        drep_constitution_threshold = cur_pparams["dRepVotingThresholds"]["ppGovGroup"]
        spo_constitution_threshold = cur_pparams["poolVotingThresholds"]["ppSecurityGroup"]
        is_drep_total_below_threshold = (
            delegated_stake.drep / delegated_stake.total_lovelace
        ) < drep_constitution_threshold
        is_spo_total_below_threshold = (
            delegated_stake.spo / delegated_stake.total_lovelace
        ) < spo_constitution_threshold

        # PParam groups

        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cip049, reqc.cip050, reqc.cip051, reqc.cip052)]

        fin_update_proposals = [
            # From network group
            clusterlib_utils.UpdateProposal(
                arg="--max-collateral-inputs",
                value=cur_pparams["maxCollateralInputs"],
                name="maxCollateralInputs",
            ),
            # From economic group
            clusterlib_utils.UpdateProposal(
                arg="--min-pool-cost",
                value=cur_pparams["minPoolCost"],
                name="minPoolCost",
            ),
            # From technical group
            clusterlib_utils.UpdateProposal(
                arg="--collateral-percent",
                value=cur_pparams["collateralPercentage"],
                name="collateralPercentage",
            ),
            # From governance group
            clusterlib_utils.UpdateProposal(
                arg="--committee-term-length",
                value=random.randint(11000, 12000),
                name="committeeMaxTermLength",
            ),
            clusterlib_utils.UpdateProposal(
                arg="--drep-activity",
                value=random.randint(101, 120),
                name="dRepActivity",
            ),
            # From security pparams
            clusterlib_utils.UpdateProposal(
                arg="--max-tx-size",
                value=cur_pparams["maxTxSize"],
                name="maxTxSize",
            ),
        ]
        if configuration.HAS_CC:
            fin_update_proposals.append(
                clusterlib_utils.UpdateProposal(
                    arg="--min-committee-size",
                    value=random.randint(3, 5),
                    name="committeeMinSize",
                )
            )
        
        # Intentionally use the same previous action for all proposals
        prev_action_rec = governance_utils.get_prev_action(
            action_type=governance_utils.PrevGovActionIds.PPARAM_UPDATE,
            gov_state=cluster.g_conway_governance.query.gov_state(),
        )
        
        def _propose_pparams_update(
            name_template: str,
            proposals: tp.List[clusterlib_utils.UpdateProposal],
        ) -> conway_common.PParamPropRec:
            anchor_url = f"http://www.pparam-action-{clusterlib.get_rand_str(4)}.com"
            anchor_data_hash = cluster.g_conway_governance.get_anchor_data_hash(text=anchor_url)
            return conway_common.propose_pparams_update(
                cluster_obj=cluster,
                name_template=name_template,
                anchor_url=anchor_url,
                anchor_data_hash=anchor_data_hash,
                pool_users=pool_users_lg,
                proposals=proposals,
                prev_action_rec=prev_action_rec,
                num_pool_users= num_pool_users
            )

        def _check_state(state: dict):
            pparams = state.get("curPParams") or state.get("currentPParams") or {}
            clusterlib_utils.check_updated_params(
                update_proposals=fin_update_proposals, protocol_params=pparams
            )
        proposed_pparams_errors = []

        def _check_proposed_pparams(
            update_proposals: tp.List[clusterlib_utils.UpdateProposal], protocol_params: dict
        ) -> None:
            try:
                clusterlib_utils.check_updated_params(
                    update_proposals=update_proposals,
                    protocol_params=protocol_params,
                )
            except AssertionError as err:
                proposed_pparams_errors.append(str(err))

        _url = helpers.get_vcs_link()
        [
            r.start(url=_url)
            for r in (reqc.cip044, reqc.cip045, reqc.cip046, reqc.cip047, reqc.cip060)
        ]
        if configuration.HAS_CC:
            reqc.cip006.start(url=_url)
        
        try: 
            fin_prop_recs = _propose_pparams_update(
                name_template=f"{temp_template}_fin_no", proposals=fin_update_proposals
            )
            [r.success() for r in (reqc.cli017, reqc.cip031a_05, reqc.cip031e, reqc.cip054_01)]
            for fin_prop_rec in fin_prop_recs: 
                _check_proposed_pparams(
                    update_proposals=fin_prop_rec.proposals,
                    protocol_params=fin_prop_rec.future_pparams,
                )
                action_ix =  fin_prop_rec.action_ix
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_fin_yes",
                    action_ix=action_ix,
                    action_txid=fin_prop_rec.action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=conway_common.Votes.INSUFFICIENT 
                    if action_ix == 0 or action_ix == 1
                    else conway_common.Votes.MAJORITY,
                    vote_cc=True,
                    vote_drep=False if is_in_bootstrap else True,
                    vote_spo=True
                )  
            fin_approve_epoch = cluster.g_query.get_epoch()
            # Check Ratification
            reqc.cip068.start(url=helpers.get_vcs_link())
            _cur_epoch = cluster.g_query.get_epoch()
            if _cur_epoch == fin_approve_epoch:
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            if _cur_epoch == fin_approve_epoch + 1:
                rat_gov_state = cluster.g_conway_governance.query.gov_state()
                conway_common.save_gov_state(
                        gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
                    )
                for fin_prop_rec in fin_prop_recs:
                    action_ix = fin_prop_rec.action_ix
                    rat_action = governance_utils.lookup_ratified_actions(
                        gov_state=rat_gov_state, action_txid=fin_prop_rec.action_txid, action_ix=action_ix
                    )
                    if action_ix == 0 or action_ix == 1: 
                        assert not rat_action, f"Action {fin_prop_rec.action_txid}#{action_ix} ratified without enough votes"
                    else: 
                        enact_prev_action_rec = governance_utils.get_prev_action(
                            action_type=governance_utils.PrevGovActionIds.PPARAM_UPDATE,
                            gov_state=rat_gov_state,
                        )
                        enacted_action = (enact_prev_action_rec.txid == fin_prop_rec.action_txid) and (enact_prev_action_rec.ix == action_ix)
                        assert rat_action or enacted_action, f"Action {fin_prop_rec.action_txid}#{action_ix} not ratified or enacted"

                next_rat_state = rat_gov_state["nextRatifyState"]
                _check_state(next_rat_state["nextEnactState"])
                reqc.cip038_04.start(url=helpers.get_vcs_link())
                assert not next_rat_state["ratificationDelayed"], "Ratification is delayed unexpectedly"
                reqc.cip038_04.success() 
                
                # Wait for enactment
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            
            # Check enactment
            assert _cur_epoch == fin_approve_epoch + 2, f"Unexpected epoch {_cur_epoch}"
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            )
            _check_state(enact_gov_state)
            [
                r.success()
                for r in (
                    reqc.cip037,
                    reqc.cip044,
                    reqc.cip045,
                    reqc.cip046,
                    reqc.cip047,
                    reqc.cip049,
                    reqc.cip050,
                    reqc.cip051,
                    reqc.cip052,
                    reqc.cip056,
                    reqc.cip060,
                    reqc.cip061_02,
                    reqc.cip061_04,
                    reqc.cip065,
                    reqc.cip068,
                    reqc.cip074,
                )
            ]
            if configuration.HAS_CC:
                reqc.cip006.success()
                reqc.cip062_01.success()
                reqc.cip062_02.success()
            if is_drep_total_below_threshold:
                reqc.cip064_03.success()
            if is_spo_total_below_threshold:
                reqc.cip064_04.success()
            if proposed_pparams_errors:
                proposed_pparams_errors_str = "\n".join(proposed_pparams_errors)
                raise AssertionError(proposed_pparams_errors_str)
            # try voting on enacted action
            with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common.cast_vote(
                        cluster_obj=cluster,
                        governance_data=governance_data,
                        name_template=f"{temp_template}_enacted",
                        payment_addr=pool_users_lg[0].payment,
                        action_txid=fin_prop_recs[2].action_txid,
                        action_ix=2,
                        approve_cc=False,
                        approve_drep=None if is_in_bootstrap else False,
                    )
            err_str = str(excinfo.value)
            assert "(GovActionsDoNotExist" in err_str, err_str
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at proposing {(num_pool_users)} parameter updates actions in a single transaction")
                return

    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_pparam_update_equal(
        self,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: tp.List[clusterlib.PoolUser], 
    ):
        """Test protocol parameter update.

        * submit 3 "protocol parameters update" action
        * 3 SPOs, 90 CC Members and 100 DReps vote insufficiently to disapprove action 1 
        * 3 SPOs, 90 CC Members and 100 DReps vote majority to approve action 2 and 3
        * check that the first action on the proposal list with enough votes is enacted
        """
        
        num_pool_users = 3
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_lock_governance
        temp_template = common.get_test_id(cluster)
        cost_proposal_file = DATA_DIR / "cost_models_list.json"
        is_in_bootstrap = conway_common.is_in_bootstrap(cluster_obj=cluster)

        if is_in_bootstrap and not configuration.HAS_CC:
            pytest.skip("The test doesn't work in bootstrap period without CC.")

        # Check if total delegated stake is below the threshold. This can be used to check that
        # undelegated stake is treated as Abstain. If undelegated stake was treated as Yes, than
        # missing votes would approve the action.
        delegated_stake = governance_utils.get_delegated_stake(cluster_obj=cluster)
        cur_pparams = cluster.g_conway_governance.query.gov_state()["currentPParams"]
        drep_constitution_threshold = cur_pparams["dRepVotingThresholds"]["ppGovGroup"]
        spo_constitution_threshold = cur_pparams["poolVotingThresholds"]["ppSecurityGroup"]
        is_drep_total_below_threshold = (
            delegated_stake.drep / delegated_stake.total_lovelace
        ) < drep_constitution_threshold
        is_spo_total_below_threshold = (
            delegated_stake.spo / delegated_stake.total_lovelace
        ) < spo_constitution_threshold

        # PParam groups

        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cip049, reqc.cip050, reqc.cip051, reqc.cip052)]

        fin_update_proposals = [
            # From network group
            clusterlib_utils.UpdateProposal(
                arg="--max-collateral-inputs",
                value=cur_pparams["maxCollateralInputs"],
                name="maxCollateralInputs",
            ),
            # From economic group
            clusterlib_utils.UpdateProposal(
                arg="--min-pool-cost",
                value=cur_pparams["minPoolCost"],
                name="minPoolCost",
            ),
            # From technical group
            clusterlib_utils.UpdateProposal(
                arg="--collateral-percent",
                value=cur_pparams["collateralPercentage"],
                name="collateralPercentage",
            ),
            # From governance group
            clusterlib_utils.UpdateProposal(
                arg="--committee-term-length",
                value=random.randint(11000, 12000),
                name="committeeMaxTermLength",
            ),
            clusterlib_utils.UpdateProposal(
                arg="--drep-activity",
                value=random.randint(101, 120),
                name="dRepActivity",
            ),
            # From security pparams
            clusterlib_utils.UpdateProposal(
                arg="--max-tx-size",
                value=cur_pparams["maxTxSize"],
                name="maxTxSize",
            ),
        ]
        if configuration.HAS_CC:
            fin_update_proposals.append(
                clusterlib_utils.UpdateProposal(
                    arg="--min-committee-size",
                    value=random.randint(3, 5),
                    name="committeeMinSize",
                )
            )
        
        # Intentionally use the same previous action for all proposals
        prev_action_rec = governance_utils.get_prev_action(
            action_type=governance_utils.PrevGovActionIds.PPARAM_UPDATE,
            gov_state=cluster.g_conway_governance.query.gov_state(),
        )
        
        def _propose_pparams_update(
            name_template: str,
            proposals: tp.List[clusterlib_utils.UpdateProposal],
        ) -> conway_common.PParamPropRec:
            anchor_url = f"http://www.pparam-action-{clusterlib.get_rand_str(4)}.com"
            anchor_data_hash = cluster.g_conway_governance.get_anchor_data_hash(text=anchor_url)
            return conway_common.propose_pparams_update(
                cluster_obj=cluster,
                name_template=name_template,
                anchor_url=anchor_url,
                anchor_data_hash=anchor_data_hash,
                pool_users=pool_users_lg,
                proposals=proposals,
                prev_action_rec=prev_action_rec,
                num_pool_users= num_pool_users
            )

        def _check_state(state: dict):
            pparams = state.get("curPParams") or state.get("currentPParams") or {}
            clusterlib_utils.check_updated_params(
                update_proposals=fin_update_proposals, protocol_params=pparams
            )
        proposed_pparams_errors = []

        def _check_proposed_pparams(
            update_proposals: tp.List[clusterlib_utils.UpdateProposal], protocol_params: dict
        ) -> None:
            try:
                clusterlib_utils.check_updated_params(
                    update_proposals=update_proposals,
                    protocol_params=protocol_params,
                )
            except AssertionError as err:
                proposed_pparams_errors.append(str(err))

        _url = helpers.get_vcs_link()
        [
            r.start(url=_url)
            for r in (reqc.cip044, reqc.cip045, reqc.cip046, reqc.cip047, reqc.cip060)
        ]
        if configuration.HAS_CC:
            reqc.cip006.start(url=_url)
        
        try: 
            fin_prop_recs = _propose_pparams_update(
                name_template=f"{temp_template}_fin_no", proposals=fin_update_proposals
            )
            [r.success() for r in (reqc.cli017, reqc.cip031a_05, reqc.cip031e, reqc.cip054_01)]
            for fin_prop_rec in fin_prop_recs: 
                _check_proposed_pparams(
                    update_proposals=fin_prop_rec.proposals,
                    protocol_params=fin_prop_rec.future_pparams,
                )
                action_ix =  fin_prop_rec.action_ix
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_fin_yes",
                    action_ix=action_ix,
                    action_txid=fin_prop_rec.action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=conway_common.Votes.INSUFFICIENT 
                    if action_ix == 0
                    else conway_common.Votes.MAJORITY,
                    vote_cc=True,
                    vote_drep=False if is_in_bootstrap else True,
                    vote_spo=True
                )  
            fin_approve_epoch = cluster.g_query.get_epoch()
            # Check Ratification
            reqc.cip068.start(url=helpers.get_vcs_link())
            _cur_epoch = cluster.g_query.get_epoch()
            if _cur_epoch == fin_approve_epoch:
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            if _cur_epoch == fin_approve_epoch + 1:
                rat_gov_state = cluster.g_conway_governance.query.gov_state()
                conway_common.save_gov_state(
                        gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
                    )
                for fin_prop_rec in fin_prop_recs:
                    action_ix = fin_prop_rec.action_ix
                    rat_action = governance_utils.lookup_ratified_actions(
                        gov_state=rat_gov_state, action_txid=fin_prop_rec.action_txid, action_ix=action_ix
                    )
                    if action_ix == 0: 
                        assert not rat_action, f"Action {fin_prop_rec.action_txid}#{action_ix} ratified without enough votes"
                    elif action_ix == 2: 
                        assert not rat_action, f"Action {fin_prop_rec.action_txid}#{action_ix} ratified without being submitted first"
                    else: 
                        enact_prev_action_rec = governance_utils.get_prev_action(
                            action_type=governance_utils.PrevGovActionIds.PPARAM_UPDATE,
                            gov_state=rat_gov_state,
                        )
                        enacted_action = (enact_prev_action_rec.txid == fin_prop_rec.action_txid) and (enact_prev_action_rec.ix == action_ix)
                        assert rat_action or enacted_action, f"Action {fin_prop_rec.action_txid}#{action_ix} not found in ratified action"

                next_rat_state = rat_gov_state["nextRatifyState"]
                _check_state(next_rat_state["nextEnactState"])
                reqc.cip038_04.start(url=helpers.get_vcs_link())
                assert not next_rat_state["ratificationDelayed"], "Ratification is delayed unexpectedly"
                reqc.cip038_04.success() 
                
                # Wait for enactment
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            
            # Check enactment
            assert _cur_epoch == fin_approve_epoch + 2, f"Unexpected epoch {_cur_epoch}"
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                gov_state=enact_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
            )
            _check_state(enact_gov_state)
            [
                r.success()
                for r in (
                    reqc.cip037,
                    reqc.cip044,
                    reqc.cip045,
                    reqc.cip046,
                    reqc.cip047,
                    reqc.cip049,
                    reqc.cip050,
                    reqc.cip051,
                    reqc.cip052,
                    reqc.cip056,
                    reqc.cip060,
                    reqc.cip061_02,
                    reqc.cip061_04,
                    reqc.cip065,
                    reqc.cip068,
                    reqc.cip074,
                )
            ]
            if configuration.HAS_CC:
                reqc.cip006.success()
                reqc.cip062_01.success()
                reqc.cip062_02.success()
            if is_drep_total_below_threshold:
                reqc.cip064_03.success()
            if is_spo_total_below_threshold:
                reqc.cip064_04.success()
            if proposed_pparams_errors:
                proposed_pparams_errors_str = "\n".join(proposed_pparams_errors)
                raise AssertionError(proposed_pparams_errors_str)
            # try voting on enacted action
            with pytest.raises(clusterlib.CLIError) as excinfo:
                    conway_common.cast_vote(
                        cluster_obj=cluster,
                        governance_data=governance_data,
                        name_template=f"{temp_template}_enacted",
                        payment_addr=pool_users_lg[0].payment,
                        action_txid=fin_prop_recs[2].action_txid,
                        action_ix=1,
                        approve_cc=False,
                        approve_drep=None if is_in_bootstrap else False,
                    )
            err_str = str(excinfo.value)
            assert "(GovActionsDoNotExist" in err_str, err_str
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at proposing {(num_pool_users)} parameter updates actions in a single transaction")
                return
    
    @allure.link(helpers.get_vcs_link())
    @pytest.mark.long
    @pytest.mark.load_test
    def test_pparam_update_insufficient(
        self,
        cluster_lock_governance: governance_setup.GovClusterT,
        pool_users_lg: tp.List[clusterlib.PoolUser], 
    ):
        """Test protocol parameter update.

        * submit 3 "protocol parameters update" action
        * 3 SPOs, 90 CC Members and 100 DReps vote insufficiently to disapprove all actions
        * check that none of the actions are ratified or enacted
        """
        
        num_pool_users = 3
        # pylint: disable=too-many-locals,too-many-statements
        cluster, governance_data = cluster_lock_governance
        temp_template = common.get_test_id(cluster)
        cost_proposal_file = DATA_DIR / "cost_models_list.json"
        is_in_bootstrap = conway_common.is_in_bootstrap(cluster_obj=cluster)

        if is_in_bootstrap and not configuration.HAS_CC:
            pytest.skip("The test doesn't work in bootstrap period without CC.")

        # Check if total delegated stake is below the threshold. This can be used to check that
        # undelegated stake is treated as Abstain. If undelegated stake was treated as Yes, than
        # missing votes would approve the action.
        delegated_stake = governance_utils.get_delegated_stake(cluster_obj=cluster)
        cur_pparams = cluster.g_conway_governance.query.gov_state()["currentPParams"]
        drep_constitution_threshold = cur_pparams["dRepVotingThresholds"]["ppGovGroup"]
        spo_constitution_threshold = cur_pparams["poolVotingThresholds"]["ppSecurityGroup"]
        is_drep_total_below_threshold = (
            delegated_stake.drep / delegated_stake.total_lovelace
        ) < drep_constitution_threshold
        is_spo_total_below_threshold = (
            delegated_stake.spo / delegated_stake.total_lovelace
        ) < spo_constitution_threshold
        
        # PParam groups
        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cip049, reqc.cip050, reqc.cip051, reqc.cip052)]

        fin_update_proposals = [
            # From network group
            clusterlib_utils.UpdateProposal(
                arg="--max-collateral-inputs",
                value=cur_pparams["maxCollateralInputs"],
                name="maxCollateralInputs",
            ),
            # From economic group
            clusterlib_utils.UpdateProposal(
                arg="--min-pool-cost",
                value=cur_pparams["minPoolCost"],
                name="minPoolCost",
            ),
            # From technical group
            clusterlib_utils.UpdateProposal(
                arg="--collateral-percent",
                value=cur_pparams["collateralPercentage"],
                name="collateralPercentage",
            ),
            # From governance group
            clusterlib_utils.UpdateProposal(
                arg="--committee-term-length",
                value=random.randint(11000, 12000),
                name="committeeMaxTermLength",
            ),
            clusterlib_utils.UpdateProposal(
                arg="--drep-activity",
                value=random.randint(101, 120),
                name="dRepActivity",
            ),
            # From security pparams
            clusterlib_utils.UpdateProposal(
                arg="--max-tx-size",
                value=cur_pparams["maxTxSize"],
                name="maxTxSize",
            ),
        ]
        if configuration.HAS_CC:
            fin_update_proposals.append(
                clusterlib_utils.UpdateProposal(
                    arg="--min-committee-size",
                    value=random.randint(3, 5),
                    name="committeeMinSize",
                )
            )
        
        # Intentionally use the same previous action for all proposals
        prev_action_rec = governance_utils.get_prev_action(
            action_type=governance_utils.PrevGovActionIds.PPARAM_UPDATE,
            gov_state=cluster.g_conway_governance.query.gov_state(),
        )
        
        def _propose_pparams_update(
            name_template: str,
            proposals: tp.List[clusterlib_utils.UpdateProposal],
        ) -> conway_common.PParamPropRec:
            anchor_url = f"http://www.pparam-action-{clusterlib.get_rand_str(4)}.com"
            anchor_data_hash = cluster.g_conway_governance.get_anchor_data_hash(text=anchor_url)
            return conway_common.propose_pparams_update(
                cluster_obj=cluster,
                name_template=name_template,
                anchor_url=anchor_url,
                anchor_data_hash=anchor_data_hash,
                pool_users=pool_users_lg,
                proposals=proposals,
                prev_action_rec=prev_action_rec,
                num_pool_users= num_pool_users
            )

        def _check_state(state: dict):
            pparams = state.get("curPParams") or state.get("currentPParams") or {}
            clusterlib_utils.check_updated_params(
                update_proposals=fin_update_proposals, protocol_params=pparams
            )
        proposed_pparams_errors = []

        def _check_proposed_pparams(
            update_proposals: tp.List[clusterlib_utils.UpdateProposal], protocol_params: dict
        ) -> None:
            try:
                clusterlib_utils.check_updated_params(
                    update_proposals=update_proposals,
                    protocol_params=protocol_params,
                )
            except AssertionError as err:
                proposed_pparams_errors.append(str(err))

        _url = helpers.get_vcs_link()
        [
            r.start(url=_url)
            for r in (reqc.cip044, reqc.cip045, reqc.cip046, reqc.cip047, reqc.cip060)
        ]
        if configuration.HAS_CC:
            reqc.cip006.start(url=_url)
        
        try: 
            fin_prop_recs = _propose_pparams_update(
                name_template=f"{temp_template}_fin_no", proposals=fin_update_proposals
            )
            [r.success() for r in (reqc.cli017, reqc.cip031a_05, reqc.cip031e, reqc.cip054_01)]
            for fin_prop_rec in fin_prop_recs: 
                _check_proposed_pparams(
                    update_proposals=fin_prop_rec.proposals,
                    protocol_params=fin_prop_rec.future_pparams,
                )
                action_ix =  fin_prop_rec.action_ix
                conway_common._cast_vote(
                    temp_template=f"{temp_template}_fin_yes",
                    action_ix=action_ix,
                    action_txid=fin_prop_rec.action_txid,
                    governance_data=governance_data,
                    cluster=cluster,
                    pool_user=pool_users_lg[0],
                    vote=conway_common.Votes.INSUFFICIENT,
                    vote_cc=True,
                    vote_drep=False if is_in_bootstrap else True,
                    vote_spo=True
                )  
            fin_approve_epoch = cluster.g_query.get_epoch()
            # Check Ratification
            reqc.cip068.start(url=helpers.get_vcs_link())
            _cur_epoch = cluster.g_query.get_epoch()
            if _cur_epoch == fin_approve_epoch:
                _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            if _cur_epoch == fin_approve_epoch + 1:
                rat_gov_state = cluster.g_conway_governance.query.gov_state()
                conway_common.save_gov_state(
                        gov_state=rat_gov_state, name_template=f"{temp_template}_rat_{_cur_epoch}"
                    )
                for fin_prop_rec in fin_prop_recs:
                    action_ix = fin_prop_rec.action_ix
                    rat_action = governance_utils.lookup_ratified_actions(
                        gov_state=rat_gov_state, action_txid=fin_prop_rec.action_txid, action_ix=action_ix
                    )
                    assert not rat_action, f"Action {fin_prop_rec.action_txid}#{action_ix} ratified without enough votes"

                next_PParams = rat_gov_state["nextRatifyState"]["nextEnactState"]["curPParams"]
                assert cur_pparams == next_PParams, "Parameters Changed with insufficient voting"
            
            #Check enactment 
            _cur_epoch = cluster.wait_for_new_epoch(padding_seconds=5)
            enact_gov_state = cluster.g_conway_governance.query.gov_state()
            conway_common.save_gov_state(
                        gov_state=rat_gov_state, name_template=f"{temp_template}_enact_{_cur_epoch}"
                    )
            enact_gov_state_pparams = enact_gov_state["currentPParams"]
            assert enact_gov_state_pparams == cur_pparams, "Parameters Changed with insufficient voting"
            
        except clusterlib.CLIError as exc:
            err_str = str(exc)
            if "MaxTxSizeUTxO" in err_str:
                print(f"Fails at proposing {(num_pool_users)} parameter updates actions in a single transaction")
                return
