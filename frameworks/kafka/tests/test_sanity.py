import logging
import pytest
import retrying
import sdk_cmd
import sdk_hosts
import sdk_install
import sdk_marathon
import sdk_metrics
import sdk_plan
import sdk_tasks
import sdk_upgrade
import sdk_utils
import shakedown
from tests import config, test_utils

log = logging.getLogger(__name__)
foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)


@pytest.fixture(scope="module", autouse=True)
def configure_package(configure_security):
    try:
        log.info("Ensure kafka is uninstalled...")
        sdk_install.uninstall(config.PACKAGE_NAME, foldered_name)

        sdk_upgrade.test_upgrade(
            config.PACKAGE_NAME,
            foldered_name,
            config.DEFAULT_BROKER_COUNT,
            from_options={"service": {"name": foldered_name}, "brokers": {"cpus": 0.5}},
        )

        # wait for brokers to finish registering before starting tests
        test_utils.broker_count_check(config.DEFAULT_BROKER_COUNT, service_name=foldered_name)

        yield  # let the test session execute
    finally:
        return


@pytest.mark.sanity
@pytest.mark.smoke
def test_service_health():
    assert shakedown.service_healthy(sdk_utils.get_foldered_name(config.SERVICE_NAME))

# --------- Endpoints -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_endpoints_address():
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)

    @retrying.retry(wait_fixed=1000, stop_max_delay=120 * 1000, retry_on_result=lambda res: not res)
    def wait():
        _, ret, _ = sdk_cmd.svc_cli(
            config.PACKAGE_NAME,
            foldered_name,
            "endpoints {}".format(config.DEFAULT_TASK_NAME),
            parse_json=True,
        )
        if len(ret["address"]) == config.DEFAULT_BROKER_COUNT:
            return ret
        return False

    endpoints = wait()
    # NOTE: do NOT closed-to-extension assert len(endpoints) == _something_
    assert len(endpoints["address"]) == config.DEFAULT_BROKER_COUNT
    assert len(endpoints["dns"]) == config.DEFAULT_BROKER_COUNT
    for i in range(len(endpoints["dns"])):
        assert (
            sdk_hosts.autoip_host(foldered_name, "kafka-{}-broker".format(i)) in endpoints["dns"][i]
        )
    assert endpoints["vip"] == sdk_hosts.vip_host(foldered_name, "broker", 9092)


@pytest.mark.smoke
@pytest.mark.sanity
def test_endpoints_zookeeper_default():
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
    _, zookeeper, _ = sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "endpoints zookeeper")
    assert zookeeper.rstrip("\n") == "master.mesos:2181/{}".format(
        sdk_utils.get_zk_path(foldered_name)
    )


@pytest.mark.smoke
@pytest.mark.sanity
def test_custom_zookeeper():
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
    broker_ids = sdk_tasks.get_task_ids(foldered_name, "{}-".format(config.DEFAULT_POD_TYPE))

    # create a topic against the default zk:
    test_utils.create_topic(config.DEFAULT_TOPIC_NAME, service_name=foldered_name)

    marathon_config = sdk_marathon.get_config(foldered_name)
    # should be using default path when this envvar is empty/unset:
    assert marathon_config["env"]["KAFKA_ZOOKEEPER_URI"] == ""

    # use a custom zk path that's WITHIN the 'dcos-service-' path, so that it's automatically cleaned up in uninstall:
    zk_path = "master.mesos:2181/{}/CUSTOMPATH".format(sdk_utils.get_zk_path(foldered_name))
    marathon_config["env"]["KAFKA_ZOOKEEPER_URI"] = zk_path
    sdk_marathon.update_app(marathon_config)

    sdk_tasks.check_tasks_updated(foldered_name, "{}-".format(config.DEFAULT_POD_TYPE), broker_ids)
    sdk_plan.wait_for_completed_deployment(foldered_name)

    # wait for brokers to finish registering
    test_utils.broker_count_check(config.DEFAULT_BROKER_COUNT, service_name=foldered_name)

    _, zookeeper, _ = sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "endpoints zookeeper")
    assert zookeeper.rstrip("\n") == zk_path

    # topic created earlier against default zk should no longer be present:
    _, topic_list_info, _ = sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "topic list", parse_json=True)

    test_utils.assert_topic_lists_are_equal_without_automatic_topics([], topic_list_info)

    # tests from here continue with the custom ZK path...


# --------- Broker -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_broker_list():
    _, brokers, _ = sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        sdk_utils.get_foldered_name(config.SERVICE_NAME),
        "broker list",
        parse_json=True,
    )
    assert set(brokers) == set([str(i) for i in range(config.DEFAULT_BROKER_COUNT)])


@pytest.mark.smoke
@pytest.mark.sanity
def test_broker_invalid():
    rc, stdout , stderr = sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        sdk_utils.get_foldered_name(config.SERVICE_NAME),
        "broker get {}".format(config.DEFAULT_BROKER_COUNT + 1),
        parse_json=False,
    )
    assert rc == 1, "return code should be 1"
    assert "404" in stdout


# --------- Pods -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_pods_restart():
    test_utils.restart_broker_pods(sdk_utils.get_foldered_name(config.SERVICE_NAME))


@pytest.mark.smoke
@pytest.mark.sanity
def test_pod_replace():
    test_utils.replace_broker_pod(sdk_utils.get_foldered_name(config.SERVICE_NAME))


# --------- CLI -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_help_cli():
    sdk_cmd.svc_cli(config.PACKAGE_NAME, sdk_utils.get_foldered_name(config.SERVICE_NAME), "help")


@pytest.mark.smoke
@pytest.mark.sanity
def test_config_cli():
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
    _, configs, _ = sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "config list", parse_json=True)
    # refrain from breaking this test if earlier tests did a config update
    assert len(configs) >= 1

    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME, foldered_name, "config show {}".format(configs[0]), print_output=False
    )[1]  # noisy output
    assert sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "config target", parse_json=True)[1]
    assert sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "config target_id", parse_json=True)[1]


@pytest.mark.smoke
@pytest.mark.sanity
def test_plan_cli():
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
    assert sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "plan list", parse_json=True)[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME, foldered_name, "plan show {}".format(config.DEFAULT_PLAN_NAME)
    )[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        foldered_name,
        "plan show --json {}".format(config.DEFAULT_PLAN_NAME),
        parse_json=True,
    )[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        foldered_name,
        "plan show {} --json".format(config.DEFAULT_PLAN_NAME),
        parse_json=True,
    )[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME, foldered_name, "plan force-restart {}".format(config.DEFAULT_PLAN_NAME)
    )[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        foldered_name,
        "plan interrupt {} {}".format(config.DEFAULT_PLAN_NAME, config.DEFAULT_PHASE_NAME),
    )[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        foldered_name,
        "plan continue {} {}".format(config.DEFAULT_PLAN_NAME, config.DEFAULT_PHASE_NAME),
    )[1]


@pytest.mark.smoke
@pytest.mark.sanity
def test_state_cli():
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
    assert sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "state framework_id", parse_json=True)[1]
    assert sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "state properties", parse_json=True)[1]


@pytest.mark.smoke
@pytest.mark.sanity
def test_pod_cli():
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
    assert sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "pod list", parse_json=True)[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        foldered_name,
        "pod status --json {}-0".format(config.DEFAULT_POD_TYPE),
        parse_json=True,
    )[1]
    assert sdk_cmd.svc_cli(
        config.PACKAGE_NAME,
        foldered_name,
        "pod info {}-0".format(config.DEFAULT_POD_TYPE),
        print_output=False,
    )[1]  # noisy output


@pytest.mark.sanity
@pytest.mark.metrics
@pytest.mark.dcos_min_version("1.9")
def test_metrics():
    expected_metrics = [
        "kafka.network.RequestMetrics.ResponseQueueTimeMs.max",
        "kafka.socket-server-metrics.io-ratio",
        "kafka.controller.ControllerStats.LeaderElectionRateAndTimeMs.p95",
    ]

    def expected_metrics_exist(emitted_metrics):
        return sdk_metrics.check_metrics_presence(emitted_metrics, expected_metrics)

    pod_name = "{}-0".format(config.DEFAULT_POD_TYPE)
    foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
    sdk_metrics.wait_for_service_metrics(
        config.PACKAGE_NAME,
        foldered_name,
        pod_name,
        "kafka-0-broker",
        config.DEFAULT_KAFKA_TIMEOUT,
        expected_metrics_exist,
    )

@pytest.mark.sanity
def test_uninstall_service():
    log.info("Clean up kafka...")
    sdk_install.uninstall(config.PACKAGE_NAME, foldered_name)
