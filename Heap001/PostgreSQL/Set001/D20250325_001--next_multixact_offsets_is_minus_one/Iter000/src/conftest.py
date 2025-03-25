# /////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

import _pytest.outcomes
from .TestConfigProp import TestConfigPropNames
from .TestStartupData import TestStartupData

import pluggy
import pytest
import _pytest
import os
import logging
import pathlib
import math

from datetime import datetime
from datetime import timedelta

# /////////////////////////////////////////////////////////////////////////////
# Fixtures


# /////////////////////////////////////////////////////////////////////////////
# TEST_PROCESS_STATS


class TEST_PROCESS_STATS:
    cTotalTests: int = 0
    cNotExecutedTests: int = 0
    cExecutedTests: int = 0
    cPassedTests: int = 0
    cFailedTests: int = 0
    cXFailedTests: int = 0
    cSkippedTests: int = 0
    cNotXFailedTests: int = 0
    cUnexpectedTests: int = 0

    FailedTests = list[str]()
    XFailedTests = list[str]()
    NotXFailedTests = list[str]()

    # --------------------------------------------------------------------
    def incrementTotalTestCount() -> None:
        __class__.cTotalTests += 1

    # --------------------------------------------------------------------
    def incrementNotExecutedTestCount() -> None:
        __class__.cNotExecutedTests += 1

    # --------------------------------------------------------------------
    def incrementExecutedTestCount() -> int:
        __class__.cExecutedTests += 1
        return __class__.cExecutedTests

    # --------------------------------------------------------------------
    def incrementPassedTestCount() -> None:
        __class__.cPassedTests += 1

    # --------------------------------------------------------------------
    def incrementFailedTestCount(testID: str) -> None:
        assert type(testID) == str
        assert type(__class__.FailedTests) == list

        __class__.FailedTests.append(testID)  # raise?
        __class__.cFailedTests += 1

    # --------------------------------------------------------------------
    def incrementXFailedTestCount(testID: str) -> None:
        assert type(testID) == str
        assert type(__class__.XFailedTests) == list

        __class__.XFailedTests.append(testID)  # raise?
        __class__.cXFailedTests += 1

    # --------------------------------------------------------------------
    def incrementSkippedTestCount() -> None:
        __class__.cSkippedTests += 1

    # --------------------------------------------------------------------
    def incrementNotXFailedTests(testID: str) -> None:
        assert type(testID) == str
        assert type(__class__.NotXFailedTests) == list

        __class__.NotXFailedTests.append(testID)  # raise?
        __class__.cNotXFailedTests += 1

    # --------------------------------------------------------------------
    def incrementUnexpectedTests() -> None:
        __class__.cUnexpectedTests += 1


# /////////////////////////////////////////////////////////////////////////////


def timedelta_to_human_text(delta: timedelta) -> str:
    assert isinstance(delta, timedelta)

    C_SECONDS_IN_MINUTE = 60
    C_SECONDS_IN_HOUR = 60 * C_SECONDS_IN_MINUTE

    v = delta.seconds

    cHours = int(v / C_SECONDS_IN_HOUR)
    v = v - cHours * C_SECONDS_IN_HOUR
    cMinutes = int(v / C_SECONDS_IN_MINUTE)
    cSeconds = v - cMinutes * C_SECONDS_IN_MINUTE

    result = "" if delta.days == 0 else "{0} day(s) ".format(delta.days)

    result = result + "{:02d}:{:02d}:{:02d}.{:06d}".format(
        cHours, cMinutes, cSeconds, delta.microseconds
    )

    return result


# /////////////////////////////////////////////////////////////////////////////


def helper__makereport__setup(
    item: pytest.Function, call: pytest.CallInfo, outcome: pluggy.Result
):
    assert item is not None
    assert call is not None
    assert outcome is not None
    assert type(item) == pytest.Function
    assert type(call) == pytest.CallInfo
    assert type(outcome) == pluggy.Result

    # logging.info("pytest_runtest_makereport - setup")

    testID = ""

    if item.cls is not None:
        testID = item.cls.__module__ + "." + item.cls.__name__ + "::"

    testID = testID + item.name

    TEST_PROCESS_STATS.incrementTotalTestCount()

    rep: pytest.TestReport = outcome.get_result()
    assert rep is not None
    assert type(rep) == pytest.TestReport

    if rep.outcome == "skipped":
        TEST_PROCESS_STATS.incrementNotExecutedTestCount()
        return

    if rep.outcome == "failed":
        TEST_PROCESS_STATS.incrementFailedTestCount(testID)
        return

    assert rep.outcome == "passed"

    testNumber = TEST_PROCESS_STATS.incrementExecutedTestCount()

    if testNumber > 1:
        logging.info("")

    logging.info("******************************************************")
    logging.info("* START TEST {0}".format(testID))
    logging.info("*")
    logging.info("* Path  : {0}".format(item.path))
    logging.info("* Number: {0}".format(testNumber))
    logging.info("*")


# ------------------------------------------------------------------------
def helper__makereport__call(
    item: pytest.Function, call: pytest.CallInfo, outcome: pluggy.Result
):
    assert item is not None
    assert call is not None
    assert outcome is not None
    assert type(item) == pytest.Function
    assert type(call) == pytest.CallInfo
    assert type(outcome) == pluggy.Result

    # logging.info("pytest_runtest_makereport - call")

    rep = outcome.get_result()
    assert rep is not None
    assert type(rep) == pytest.TestReport

    # --------
    testID = ""

    if item.cls is not None:
        testID = item.cls.__module__ + "." + item.cls.__name__ + "::"

    testID = testID + item.name

    # --------
    assert call.start <= call.stop

    startDT = datetime.fromtimestamp(call.start)
    assert type(startDT) == datetime
    stopDT = datetime.fromtimestamp(call.stop)
    assert type(stopDT) == datetime

    testDurration = stopDT - startDT
    assert type(testDurration) == timedelta

    # --------
    exitStatus = None
    if rep.outcome == "skipped":
        assert call.excinfo is not None  # research
        assert call.excinfo.value is not None  # research

        if type(call.excinfo.value) == _pytest.outcomes.Skipped:
            assert not hasattr(rep, "wasxfail")

            TEST_PROCESS_STATS.incrementSkippedTestCount()

            exitStatus = "SKIPPED"
            reasonText = str(call.excinfo.value)
            reasonMsg = "SKIP REASON: {0}"

        elif type(call.excinfo.value) == _pytest.outcomes.XFailed:
            TEST_PROCESS_STATS.incrementXFailedTestCount(testID)

            exitStatus = "XFAILED"
            reasonText = str(call.excinfo.value)
            reasonMsg = "XFAIL REASON: {0}"
        else:
            exitStatus = "XFAILED"
            assert hasattr(rep, "wasxfail")
            assert rep.wasxfail is not None
            assert type(rep.wasxfail) == str

            TEST_PROCESS_STATS.incrementXFailedTestCount(testID)

            reasonText = rep.wasxfail
            reasonMsg = "XFAIL REASON: {0}"

            logging.error(call.excinfo.value)

        if reasonText != "":
            logging.info("*")
            logging.info("* " + reasonMsg.format(reasonText))

    elif rep.outcome == "failed":
        assert call.excinfo is not None
        assert call.excinfo.value is not None

        TEST_PROCESS_STATS.incrementFailedTestCount(testID)

        logging.error(call.excinfo.value)
        exitStatus = "FAILED"
    elif rep.outcome == "passed":
        assert call.excinfo is None

        if hasattr(rep, "wasxfail"):
            assert type(rep.wasxfail) == str

            TEST_PROCESS_STATS.incrementNotXFailedTests(testID)

            warnMsg = "Test is marked as xfail"

            if rep.wasxfail != "":
                warnMsg += " [" + rep.wasxfail + "]"

            logging.warning(warnMsg)
            exitStatus = "NOT XFAILED"
        else:
            assert not hasattr(rep, "wasxfail")

            TEST_PROCESS_STATS.incrementPassedTestCount()
            exitStatus = "PASSED"
    else:
        TEST_PROCESS_STATS.incrementUnexpectedTests()
        exitStatus = "UNEXPECTED [{0}]".format(rep.outcome)
        assert False

    # --------
    logging.info("*")
    logging.info("* DURATION    : {0}".format(timedelta_to_human_text(testDurration)))
    logging.info("*")
    logging.info("* EXIT STATUS : {0}".format(exitStatus))
    logging.info("*")
    logging.info("* STOP TEST {0}".format(testID))


# /////////////////////////////////////////////////////////////////////////////


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Function, call: pytest.CallInfo):
    assert item is not None
    assert call is not None
    assert type(item) == pytest.Function
    assert type(call) == pytest.CallInfo

    # logging.info("[pytest_runtest_makereport][#001][{0}][{1}]".format(item.name, call.when))

    outcome: pluggy.Result = yield
    assert outcome is not None
    assert type(outcome) == pluggy.Result

    # logging.info("[pytest_runtest_makereport][#002][{0}][{1}]".format(item.name, call.when))

    rep: pytest.TestReport = outcome.get_result()
    assert rep is not None
    assert type(rep) == pytest.TestReport

    if call.when == "collect":
        return

    if call.when == "setup":
        helper__makereport__setup(item, call, outcome)
        return

    if call.when == "call":
        helper__makereport__call(item, call, outcome)
        return

    if call.when == "teardown":
        return

    assert False


# /////////////////////////////////////////////////////////////////////////////


def helper__calc_W(n: int) -> int:
    assert n > 0

    x = int(math.log10(n))
    assert type(x) == int
    assert x >= 0
    x += 1
    return x


# ------------------------------------------------------------------------
def helper__print_test_list(tests: list[str]) -> None:
    assert type(tests) == list

    assert helper__calc_W(9) == 1
    assert helper__calc_W(10) == 2
    assert helper__calc_W(11) == 2
    assert helper__calc_W(99) == 2
    assert helper__calc_W(100) == 3
    assert helper__calc_W(101) == 3
    assert helper__calc_W(999) == 3
    assert helper__calc_W(1000) == 4
    assert helper__calc_W(1001) == 4

    W = helper__calc_W(len(tests))

    templateLine = "{0:0" + str(W) + "d}. {1}"

    nTest = 0

    while nTest < len(tests):
        testID = tests[nTest]
        assert type(testID) == str
        nTest += 1
        logging.info(templateLine.format(nTest, testID))


# /////////////////////////////////////////////////////////////////////////////


@pytest.fixture(autouse=True, scope="session")
def run_after_tests(request: pytest.FixtureRequest):
    assert isinstance(request, pytest.FixtureRequest)

    yield

    logging.info("")
    logging.info("--------------------------- [FAILED TESTS]")
    logging.info("")

    assert len(TEST_PROCESS_STATS.FailedTests) == TEST_PROCESS_STATS.cFailedTests

    if len(TEST_PROCESS_STATS.FailedTests) > 0:
        helper__print_test_list(TEST_PROCESS_STATS.FailedTests)
        logging.info("")

    logging.info("--------------------------- [XFAILED TESTS]")
    logging.info("")

    assert len(TEST_PROCESS_STATS.XFailedTests) == TEST_PROCESS_STATS.cXFailedTests

    if len(TEST_PROCESS_STATS.XFailedTests) > 0:
        helper__print_test_list(TEST_PROCESS_STATS.XFailedTests)
        logging.info("")

    logging.info("--------------------------- [NOT XFAILED TESTS]")
    logging.info("")

    assert (
        len(TEST_PROCESS_STATS.NotXFailedTests) == TEST_PROCESS_STATS.cNotXFailedTests
    )

    if len(TEST_PROCESS_STATS.NotXFailedTests) > 0:
        helper__print_test_list(TEST_PROCESS_STATS.NotXFailedTests)
        logging.info("")

    logging.info("--------------------------- [SUMMARY STATISTICS]")
    logging.info("")
    logging.info("[TESTS]")
    logging.info(" TOTAL       : {0}".format(TEST_PROCESS_STATS.cTotalTests))
    logging.info(" EXECUTED    : {0}".format(TEST_PROCESS_STATS.cExecutedTests))
    logging.info(" NOT EXECUTED: {0}".format(TEST_PROCESS_STATS.cNotExecutedTests))
    logging.info("")
    logging.info(" PASSED      : {0}".format(TEST_PROCESS_STATS.cPassedTests))
    logging.info(" FAILED      : {0}".format(TEST_PROCESS_STATS.cFailedTests))
    logging.info(" XFAILED     : {0}".format(TEST_PROCESS_STATS.cXFailedTests))
    logging.info(" NOT XFAILED : {0}".format(TEST_PROCESS_STATS.cNotXFailedTests))
    logging.info(" SKIPPED     : {0}".format(TEST_PROCESS_STATS.cSkippedTests))
    logging.info(" UNEXPECTED  : {0}".format(TEST_PROCESS_STATS.cUnexpectedTests))
    logging.info("")


# /////////////////////////////////////////////////////////////////////////////


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    assert isinstance(config, pytest.Config)

    log_name = TestStartupData.GetCurrentTestWorkerSignature()
    log_name += ".log"

    if TestConfigPropNames.TEST_CFG__LOG_DIR in os.environ:
        log_path_v = os.environ[TestConfigPropNames.TEST_CFG__LOG_DIR]
        log_path = pathlib.Path(log_path_v)
    else:
        log_path = config.rootpath.joinpath("logs")

    log_path.mkdir(exist_ok=True)

    logging_plugin = config.pluginmanager.get_plugin("logging-plugin")
    logging_plugin.set_log_path(str(log_path / log_name))


# /////////////////////////////////////////////////////////////////////////////
