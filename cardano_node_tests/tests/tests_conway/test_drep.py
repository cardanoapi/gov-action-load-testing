"""Tests for Conway governance DRep functionality."""

import dataclasses
from itertools import chain
import logging
import pathlib as pl
import pickle
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

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(
    VERSIONS.transaction_era < VERSIONS.CONWAY,
    reason="runs only with Tx era >= Conway",
)

@dataclasses.dataclass(frozen=True, order=True)
class DRepStateRecord:
    epoch_no: int
    id: str
    drep_state: governance_utils.DRepStateT


@dataclasses.dataclass(frozen=True, order=True)
class DRepRatRecord:
    id: str
    ratified: bool


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
    )

    return addr


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


def create_drep(
    name_template: str,
    cluster_obj: clusterlib.ClusterLib,
    payment_addr: clusterlib.AddressRecord,
) -> governance_utils.DRepRegistration:
    """Create a DRep."""
    reg_drep = governance_utils.get_drep_reg_record(
        cluster_obj=cluster_obj,
        name_template=name_template,
    )

    tx_files_reg = clusterlib.TxFiles(
        certificate_files=[reg_drep.registration_cert],
        signing_key_files=[payment_addr.skey_file, reg_drep.key_pair.skey_file],
    )

    clusterlib_utils.build_and_submit_tx(
        cluster_obj=cluster_obj,
        name_template=f"{name_template}_drep_reg",
        src_address=payment_addr.address,
        submit_method=submit_utils.SubmitMethods.CLI,
        use_build_cmd=True,
        tx_files=tx_files_reg,
        deposit=reg_drep.deposit,
    )

    return reg_drep


def get_custom_drep(
    name_template: str,
    cluster_manager: cluster_management.ClusterManager,
    cluster_obj: clusterlib.ClusterLib,
    payment_addr: clusterlib.AddressRecord,
    caching_key: str,
) -> governance_utils.DRepRegistration:
    """Create a custom DRep and cache it."""
    if cluster_nodes.get_cluster_type().type != cluster_nodes.ClusterType.LOCAL:
        pytest.skip("runs only on local cluster")

    with cluster_manager.cache_fixture(key=caching_key) as fixture_cache:
        if fixture_cache.value:
            return fixture_cache.value  # type: ignore

        reg_drep = create_drep(
            name_template=name_template,
            cluster_obj=cluster_obj,
            payment_addr=payment_addr,
        )
        fixture_cache.value = reg_drep

    return reg_drep


@pytest.fixture
def cluster_and_pool(
    cluster_manager: cluster_management.ClusterManager,
) -> tp.Tuple[clusterlib.ClusterLib, tp.List[str]]:
    return delegation.cluster_and_pool(cluster_manager=cluster_manager)


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


@pytest.fixture
def pool_users(
    cluster_manager: cluster_management.ClusterManager,
    cluster: clusterlib.ClusterLib,
) -> tp.List[clusterlib.PoolUser]:
    test_id = common.get_test_id(cluster)
    key = helpers.get_current_line_str()
    return get_pool_user(
        name_template=test_id, cluster_manager=cluster_manager, cluster_obj=cluster, caching_key=key, no_of_users=300
    )


@pytest.fixture
def custom_drep(
    cluster_manager: cluster_management.ClusterManager,
    cluster: clusterlib.ClusterLib,
    payment_addr: clusterlib.AddressRecord,
) -> governance_utils.DRepRegistration:
    test_id = common.get_test_id(cluster)
    key = helpers.get_current_line_str()
    return get_custom_drep(
        name_template=f"custom_drep_{test_id}",
        cluster_manager=cluster_manager,
        cluster_obj=cluster,
        payment_addr=payment_addr,
        caching_key=key,
    )


@pytest.fixture
def payment_addr_wp(
    cluster_manager: cluster_management.ClusterManager,
    cluster_and_pool: tp.Tuple[clusterlib.ClusterLib, tp.List[str]],
) -> clusterlib.AddressRecord:
    cluster, __ = cluster_and_pool
    test_id = common.get_test_id(cluster)
    key = helpers.get_current_line_str()
    return get_payment_addr(
        name_template=test_id, cluster_manager=cluster_manager, cluster_obj=cluster, caching_key=key
    )


@pytest.fixture
def pool_users_wp(
    cluster_manager: cluster_management.ClusterManager,
    cluster_and_pool: tp.Tuple[clusterlib.ClusterLib, tp.List[str]],
) -> tp.List[clusterlib.PoolUser]:
    cluster, __ = cluster_and_pool
    test_id = common.get_test_id(cluster)
    key = helpers.get_current_line_str()
    return get_pool_user(
        name_template=test_id, cluster_manager=cluster_manager, cluster_obj=cluster, caching_key=key, no_of_users=300
    )


@pytest.fixture
def custom_drep_wp(
    cluster_manager: cluster_management.ClusterManager,
    cluster_and_pool: tp.Tuple[clusterlib.ClusterLib, tp.List[str]],
    payment_addr_wp: clusterlib.AddressRecord,
) -> governance_utils.DRepRegistration:
    cluster, __ = cluster_and_pool
    test_id = common.get_test_id(cluster)
    key = helpers.get_current_line_str()
    return get_custom_drep(
        name_template=f"custom_drep_{test_id}",
        cluster_manager=cluster_manager,
        cluster_obj=cluster,
        payment_addr=payment_addr_wp,
        caching_key=key,
    )


class TestDelegDReps:
    """Tests for votes delegation to DReps."""

    @allure.link(helpers.get_vcs_link())
    @common.PARAM_USE_BUILD_CMD
    @pytest.mark.parametrize("drep", ("always_abstain", "always_no_confidence", "custom"))
    @pytest.mark.testnets
    @pytest.mark.smoke
    @pytest.mark.load_test
    def test_dreps_delegation(
        self,
        cluster: clusterlib.ClusterLib,
        payment_addr: clusterlib.AddressRecord,
        pool_users: clusterlib.PoolUser,
        custom_drep: governance_utils.DRepRegistration,
        testfile_temp_dir: pl.Path,
        request: FixtureRequest,
        use_build_cmd: bool,
        drep: str,
    ):
        """Test delegating to DReps.

        * register 66 stake addresses
        * delegate stake to following DReps:

            - always-abstain
            - always-no-confidence
            - custom DRep

        * check that the stake addresses are registered
        """
        # pylint: disable=too-many-statements,too-many-locals
        num_pool_users=66
        temp_template = common.get_test_id(cluster)
        deposit_amt = cluster.g_query.get_address_deposit()
        drep_id = custom_drep.drep_id if drep == "custom" else drep

        if drep == "custom":
            reqc_deleg = reqc.cip016
        elif drep == "always_abstain":
            reqc_deleg = reqc.cip017
        elif drep == "always_no_confidence":
            reqc_deleg = reqc.cip018
        else:
            msg = f"Unexpected DRep: {drep}"
            raise ValueError(msg)

        reqc_deleg.start(url=helpers.get_vcs_link())
        pool_users = pool_users[:num_pool_users]
        total_pool_users = num_pool_users
        # Create stake address registration cert
        reqc.cli027.start(url=helpers.get_vcs_link())
        reg_certs = [
            cluster.g_stake_address.gen_stake_addr_registration_cert(
                addr_name=f"{temp_template}_addr{i}",
                deposit_amt=deposit_amt,
                stake_vkey_file=pool_users[i].stake.vkey_file,
            )
            for i in range(total_pool_users)
        ]
        reqc.cli027.success()

        # Create vote delegation cert
        _url = helpers.get_vcs_link()
        [r.start(url=_url) for r in (reqc.cli029, reqc.cip022)]
        deleg_certs = [
            cluster.g_stake_address.gen_vote_delegation_cert(
                addr_name=f"{temp_template}_addr{i}",  # Use a unique address name
                stake_vkey_file=pool_users[i].stake.vkey_file,
                drep_key_hash=custom_drep.drep_id if drep == "custom" else "",
                always_abstain=drep == "always_abstain",
                always_no_confidence=drep == "always_no_confidence",
            )
            for i in range(total_pool_users)
        ]
        [r.success() for r in (reqc.cli029, reqc.cip022)]
        
        pool_users_skey_file = [pool_user.stake.skey_file for pool_user in pool_users]
        
        tx_files = clusterlib.TxFiles(
            certificate_files=reg_certs + deleg_certs,
            signing_key_files=[payment_addr.skey_file]+pool_users_skey_file,
        )

        # Make sure we have enough time to finish the registration/delegation in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_LEDGER_STATE
        )
        print(f"\nCreating registration and vote delegation certificates for {num_pool_users} users")
        address_utxos = [cluster.g_query.get_utxo(pool_user.payment.address) for pool_user in pool_users]
        flatenned_utxos = list(chain.from_iterable(address_utxos))
        try: 
            tx_output = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=payment_addr.address,
                txins=flatenned_utxos,
                use_build_cmd=use_build_cmd,
                tx_files=tx_files,
                deposit=(deposit_amt*total_pool_users),
            )

            # Deregister stake address so it doesn't affect stake distribution
            def _deregister():
                with helpers.change_cwd(testfile_temp_dir):
                    for pool_user in pool_users:
                        stake_addr_info = cluster.g_query.get_stake_addr_info(pool_user.stake.address)
                        if not stake_addr_info:
                            continue

                        # Deregister stake address
                        reqc.cli028.start(url=helpers.get_vcs_link())
                        stake_addr_dereg_cert = cluster.g_stake_address.gen_stake_addr_deregistration_cert(
                            addr_name=f"{temp_template}_{pool_user.stake.address}_dereg",
                            deposit_amt=deposit_amt,
                            stake_vkey_file=pool_user.stake.vkey_file,
                        )
                        tx_files_dereg = clusterlib.TxFiles(
                            certificate_files=[stake_addr_dereg_cert],
                            signing_key_files=[
                                payment_addr.skey_file,
                                pool_user.stake.skey_file,
                            ],
                        )
                        withdrawals = (
                            [
                                clusterlib.TxOut(
                                    address=pool_user.stake.address,
                                    amount=stake_addr_info.reward_account_balance,
                                )
                            ]
                            if stake_addr_info.reward_account_balance
                            else []
                        )
                        clusterlib_utils.build_and_submit_tx(
                            cluster_obj=cluster,
                            name_template=f"{temp_template}_{pool_user.stake.address}_dereg",
                            src_address=payment_addr.address,
                            use_build_cmd=use_build_cmd,
                            tx_files=tx_files_dereg,
                            withdrawals=withdrawals,
                            deposit=-deposit_amt,
                        )
                        reqc.cli028.success()


            request.addfinalizer(_deregister)

            for pool_user in pool_users:
                stake_addr_info = cluster.g_query.get_stake_addr_info(pool_user.stake.address)
                assert (
                    stake_addr_info.address
                ), f"Stake address is NOT registered: {pool_user.stake.address}"
                reqc.cli035.start(url=helpers.get_vcs_link())
                assert stake_addr_info.vote_delegation == governance_utils.get_drep_cred_name(
                    drep_id=drep_id
                ), "Votes are NOT delegated to the correct DRep"
                reqc.cli035.success()

            out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output)
            assert (
                clusterlib.filter_utxos(utxos=out_utxos, address=payment_addr.address)[0].amount
                == clusterlib.calculate_utxos_balance(tx_output.txins) - tx_output.fee - (deposit_amt * total_pool_users)
            ), f"Incorrect balance for source address `{payment_addr.address}`"

            # Check that stake address is delegated to the correct DRep.
            # This takes one epoch, so test this only for selected combinations of build command
            # and submit method, only when we are running on local testnet, and only if we are not
            # running smoke tests.
            if (
                use_build_cmd
                and cluster_nodes.get_cluster_type().type == cluster_nodes.ClusterType.LOCAL
                and "smoke" not in request.config.getoption("-m")
            ):
                for pool_user in pool_users:
                    cluster.wait_for_new_epoch(padding_seconds=5)
                    deleg_state = clusterlib_utils.get_delegation_state(cluster_obj=cluster)
                    stake_addr_hash = cluster.g_stake_address.get_stake_vkey_hash(
                        stake_vkey_file=pool_user.stake.vkey_file
                    )
                    reqc.cip020_01.start(url=helpers.get_vcs_link())
                    governance_utils.check_drep_delegation(
                        deleg_state=deleg_state, drep_id=drep_id, stake_addr_hash=stake_addr_hash
                    )
                    reqc.cip020_01.success()

                    _url = helpers.get_vcs_link()
                    [r.start(url=_url) for r in (reqc.cli034, reqc.cip025)]
                    if drep == "custom":
                        stake_distrib = cluster.g_conway_governance.query.drep_stake_distribution(
                            drep_key_hash=custom_drep.drep_id
                        )
                        stake_distrib_vkey = cluster.g_conway_governance.query.drep_stake_distribution(
                            drep_vkey_file=custom_drep.key_pair.vkey_file
                        )
                        assert (
                            stake_distrib == stake_distrib_vkey
                        ), "DRep stake distribution output mismatch"
                        assert (
                            len(stake_distrib_vkey) == 1
                        ), "Unexpected number of DRep stake distribution records"

                        assert (
                            stake_distrib_vkey[0][0] == f"drep-keyHash-{custom_drep.drep_id}"
                        ), f"The DRep distribution record doesn't match the DRep ID '{custom_drep.drep_id}'"
                    else:
                        stake_distrib = cluster.g_conway_governance.query.drep_stake_distribution()

                    deleg_amount = cluster.g_query.get_address_balance(pool_user.payment.address)
                    governance_utils.check_drep_stake_distribution(
                        distrib_state=stake_distrib,
                        drep_id=drep_id,
                        min_amount=deleg_amount,
                    )
                    [r.success() for r in (reqc.cli034, reqc.cip025)]

            reqc_deleg.success()

        except clusterlib.CLIError as exc:
                err_str = str(exc)
                if "MaxTxSizeUTxO" in err_str:
                    print(f"Fails at registering {len(reg_certs + deleg_certs)} certificates in a single transaction")
                    return

    @allure.link(helpers.get_vcs_link())
    @common.PARAM_USE_BUILD_CMD
    @pytest.mark.parametrize("drep", ("always_abstain", "always_no_confidence", "custom"))
    @pytest.mark.testnets
    @pytest.mark.smoke
    @pytest.mark.load_test
    def test_dreps_and_spo_delegation(
            self,
            cluster_and_pool: tp.Tuple[clusterlib.ClusterLib, tp.List[str]],
            payment_addr_wp: clusterlib.AddressRecord,
            pool_users_wp: tp.List[clusterlib.PoolUser],
            custom_drep_wp: governance_utils.DRepRegistration,
            testfile_temp_dir: pl.Path,
            request: pytest.FixtureRequest,
            use_build_cmd: bool,
            drep: str
    ):
        """Test delegating to DRep and SPO using single certificate.

        * register 58 stake addresses
        * delegate stake to a stake pool and to following DReps:

            - always-abstain
            - always-no-confidence
            - custom DRep

        * check that the stake addresses are registered and delegated
        """
        num_pool_users = 58
        cluster, pool_ids = cluster_and_pool
        temp_template = common.get_test_id(cluster)
        deposit_amt = cluster.g_query.get_address_deposit()
        drep_id = custom_drep_wp.drep_id if drep == "custom" else drep
        pool_users_wp = pool_users_wp[:num_pool_users]
        total_pool_users_wp = len(pool_users_wp)
        # Create stake address registration cert
        reg_certs = [
            cluster.g_stake_address.gen_stake_addr_registration_cert(
                addr_name=f"{temp_template}_addr{i}",
                deposit_amt=deposit_amt,
                stake_vkey_file=pool_users_wp[i].stake.vkey_file,
            )
            for i in range(total_pool_users_wp)
        ]
        
        # Create stake and vote delegation cert
        reqc.cli030.start(url=helpers.get_vcs_link())
        deleg_certs = [
            [
                cluster.g_stake_address.gen_stake_and_vote_delegation_cert(
                    addr_name=f"{temp_template}_addr{i}",
                    stake_vkey_file=pool_users_wp[i].stake.vkey_file,
                    stake_pool_id=pool_ids[j],
                    drep_key_hash=custom_drep_wp.drep_id if drep == "custom" else "",
                    always_abstain=drep == "always_abstain",
                    always_no_confidence=drep == "always_no_confidence",
                )
                for i in range(total_pool_users_wp)
            ]
            for j in range (len(pool_ids))
        ]
        reqc.cli030.success()
        pool_users_wp_skey_file = [pool_user_wp.stake.skey_file for pool_user_wp in pool_users_wp]
        tx_files = clusterlib.TxFiles(
            certificate_files=reg_certs + list(chain.from_iterable(deleg_certs)),
            signing_key_files=[payment_addr_wp.skey_file] + pool_users_wp_skey_file,
        )

        # Make sure we have enough time to finish the registration/delegation in one epoch
        clusterlib_utils.wait_for_epoch_interval(
            cluster_obj=cluster, start=1, stop=common.EPOCH_STOP_SEC_LEDGER_STATE
        )
        print(f"\nCreating registration, vote delegation and stake delegation certificates for {total_pool_users_wp} users")
        address_utxos = [cluster.g_query.get_utxo(pool_user.payment.address) for pool_user in pool_users_wp]
        flatenned_utxos = list(chain.from_iterable(address_utxos))
        try:
            tx_output = clusterlib_utils.build_and_submit_tx(
                cluster_obj=cluster,
                name_template=temp_template,
                src_address=payment_addr_wp.address,
                txins=flatenned_utxos,
                use_build_cmd=use_build_cmd,
                tx_files=tx_files,
                deposit=(deposit_amt * total_pool_users_wp),
            )
            
            # Deregister stake address so it doesn't affect stake distribution
            def _deregister():
                with helpers.change_cwd(testfile_temp_dir):
                    for pool_user_wp in pool_users_wp:
                        stake_addr_info = cluster.g_query.get_stake_addr_info(pool_user_wp.stake.address)
                        if not stake_addr_info:
                            continue
                        
                        # Deregister stake address
                        stake_addr_dereg_cert = cluster.g_stake_address.gen_stake_addr_deregistration_cert(
                            addr_name=f"{temp_template}_addr0",
                            deposit_amt=deposit_amt,
                            stake_vkey_file=pool_user_wp.stake.vkey_file,
                        )
                        tx_files_dereg = clusterlib.TxFiles(
                            certificate_files=[stake_addr_dereg_cert],
                            signing_key_files=[
                                payment_addr_wp.skey_file,
                                pool_user_wp.stake.skey_file,
                            ],
                        )
                        withdrawals = (
                            [
                                clusterlib.TxOut(
                                    address=pool_user_wp.stake.address,
                                    amount=stake_addr_info.reward_account_balance,
                                )
                            ]
                            if stake_addr_info.reward_account_balance
                            else []
                        )
                        clusterlib_utils.build_and_submit_tx(
                            cluster_obj=cluster,
                            name_template=f"{temp_template}_dereg",
                            src_address=payment_addr_wp.address,
                            use_build_cmd=use_build_cmd,
                            tx_files=tx_files_dereg,
                            withdrawals=withdrawals,
                            deposit=-deposit_amt,
                        )

            request.addfinalizer(_deregister)
                
            # Check that the stake address was registered and delegated
            for j in range(len(pool_ids)):
                for i in range(len(pool_users_wp)):
                    stake_addr_info = cluster.g_query.get_stake_addr_info(pool_users_wp[i].stake.address)
                    assert stake_addr_info.delegation, f"Stake address was not delegated yet: {stake_addr_info}"
                    assert pool_ids[j] == stake_addr_info.delegation, "Stake address delegated to wrong pool"
                    assert stake_addr_info.vote_delegation == governance_utils.get_drep_cred_name(
                        drep_id=drep_id
                    ), "Votes are NOT delegated to the correct DRep"
            out_utxos = cluster.g_query.get_utxo(tx_raw_output=tx_output)
            # Check the expected balance
            assert (
                clusterlib.filter_utxos(utxos=out_utxos, address=payment_addr_wp.address)[0].amount
                == clusterlib.calculate_utxos_balance(tx_output.txins) - tx_output.fee - (deposit_amt * total_pool_users_wp)
            ), f"Incorrect balance for source address `{payment_addr_wp.address}`"

            # Check that stake address is delegated to the correct DRep.
            # This takes one epoch, so test this only for selected combinations of build command
            # and submit method, only when we are running on local testnet, and only if we are not
            # running smoke tests.
            if (
                use_build_cmd
                and cluster_nodes.get_cluster_type().type == cluster_nodes.ClusterType.LOCAL
                and "smoke" not in request.config.getoption("-m")
            ):
                for pool_user_wp in pool_users_wp:
                    cluster.wait_for_new_epoch(padding_seconds=5)
                    deleg_state = clusterlib_utils.get_delegation_state(cluster_obj=cluster)
                    stake_addr_hash = cluster.g_stake_address.get_stake_vkey_hash(
                        stake_vkey_file=pool_user_wp.stake.vkey_file
                    )
                    governance_utils.check_drep_delegation(
                        deleg_state=deleg_state, drep_id=drep_id, stake_addr_hash=stake_addr_hash
                    )
        except clusterlib.CLIError as exc:
                err_str = str(exc)
                if "MaxTxSizeUTxO" in err_str:
                    print(f"Fails at registering {len(reg_certs + list(chain.from_iterable(deleg_certs)))} certificates in a single transaction")
                    return