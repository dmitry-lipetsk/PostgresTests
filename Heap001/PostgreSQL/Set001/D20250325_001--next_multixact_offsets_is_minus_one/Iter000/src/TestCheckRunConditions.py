# /////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

from __future__ import annotations

from .TestgresHelpers import TestgresHelpers

from packaging.version import Version

import pytest

# /////////////////////////////////////////////////////////////////////////////
# class TestCheckRunConditions


class TestCheckRunConditions:
    def PgResetwalIsRequired(binDir: str):
        assert type(binDir) == str

        pgVersion = TestgresHelpers.NODE__get_version_string(binDir)

        if Version(pgVersion) < Version("9.4"):
            pytest.skip(
                "Version [{0}] is not supported. Utility pg_resetwal is required.".format(
                    pgVersion
                )
            )


# /////////////////////////////////////////////////////////////////////////////
