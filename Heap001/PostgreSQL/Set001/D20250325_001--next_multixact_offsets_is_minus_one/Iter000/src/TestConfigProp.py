# /////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

from __future__ import annotations

# /////////////////////////////////////////////////////////////////////////////
# TestConfigPropNames


class TestConfigPropNames:
    # fmt: off
    TEST_CFG__OLD_BIN_DIR           = "TEST_CFG__OLD_BIN_DIR"
    TEST_CFG__NEW_BIN_DIR           = "TEST_CFG__NEW_BIN_DIR"
    TEST_CFG__TEMP_DIR              = "TEST_CFG__TEMP_DIR"
    TEST_CFG__LOG_DIR               = "TEST_CFG__LOG_DIR"
    # fmt: on


# /////////////////////////////////////////////////////////////////////////////
# TestConfigPropValues


class TestConfigPropValues:
    # fmt: off
    TEST_CFG__DBNODE_KIND__PG                   = "pg"
    TEST_CFG__DBNODE_KIND__PG_WITH_PROXIMA      = "pg_with_proxima"
    TEST_CFG__DBNODE_KIND__PG_WITH_PGBOUNCER    = "pg_with_pgbouncer"
    TEST_CFG__DBNODE_KIND__PG_WITH_ODYSSEY      = "pg_with_odyssey"
    # fmt: on


# /////////////////////////////////////////////////////////////////////////////
# TestConfigPropDefaults


class TestConfigPropDefaults:
    # fmt: off
    TEST_CFG__DBNODE_KIND         = TestConfigPropValues.TEST_CFG__DBNODE_KIND__PG_WITH_PROXIMA
    # fmt: on


# /////////////////////////////////////////////////////////////////////////////
