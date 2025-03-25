# /////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

from __future__ import annotations

from .TestStartupData import TestStartupData
from .TestStartupData import TestConfigPropNames

import os
import pytest
import logging
import time

# /////////////////////////////////////////////////////////////////////////////
# TestServices


class TestServices:
    def GetRootDir() -> str:
        return TestStartupData.GetRootDir()

    # --------------------------------------------------------------------
    def GetRootTmpDir() -> str:
        return TestStartupData.GetRootTmpDataDirForCurrentTestWorker()

    # --------------------------------------------------------------------
    def GetCurTestTmpDir(request: pytest.FixtureRequest) -> str:
        assert isinstance(request, pytest.FixtureRequest)

        rootDir = TestServices.GetRootDir()
        rootTmpDir = TestServices.GetRootTmpDir()

        # [2024-12-18] It is not a fact now.
        # assert rootTmpDir.startswith(rootDir)

        testPath = str(request.path)

        if not testPath.startswith(rootDir):
            raise Exception(
                "Root dir {0} is not found in testPath {1}.".format(rootDir, testPath)
            )

        testPath2 = testPath[len(rootDir) + 1 :]

        result = os.path.join(rootTmpDir, testPath2)

        if request.node.cls is not None:
            clsName = request.node.cls.__name__
            result = os.path.join(result, clsName)

        result = os.path.join(result, request.node.name)

        return result

    # --------------------------------------------------------------------
    def GetOldBinDir() -> str:
        return __class__.Helper__ReadEnvVariable(
            TestConfigPropNames.TEST_CFG__OLD_BIN_DIR
        )

    # --------------------------------------------------------------------
    def GetNewBinDir() -> str:
        return __class__.Helper__ReadEnvVariable(
            TestConfigPropNames.TEST_CFG__NEW_BIN_DIR
        )

    # --------------------------------------------------------------------
    def PrintExceptionOK(e: Exception):
        assert isinstance(e, Exception)

        logging.info(
            "OK. We catch an exception ({0}) - {1}".format(type(e).__name__, e)
        )

    # --------------------------------------------------------------------
    def ThrowWeWaitAnException():
        raise Exception("We wait an exception!")

    # --------------------------------------------------------------------
    def SleepWithPrint(sleepTimeInSec: float):
        logging.info("Sleep {0} second(s).".format(sleepTimeInSec))
        time.sleep(sleepTimeInSec)

    # Helper methods -----------------------------------------------------
    def Helper__ReadEnvVariable(name: str) -> str:
        assert name is not None
        assert type(name) == str
        assert name != ""

        if not (name in os.environ.keys()):
            raise RuntimeError("Env variable [{0}] is not found.".format(name))

        v = os.environ[name]

        assert v is not None
        assert type(v) == str

        if v == "":
            raise RuntimeError("Env variable [{0}] is empty.".format(name))

        return v


# /////////////////////////////////////////////////////////////////////////////
