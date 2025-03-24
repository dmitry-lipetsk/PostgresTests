# //////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

from __future__ import annotations

from ...TestgresHelpers import TestgresHelpers
from ...TestgresHelpers import TestgresNode
from ...TestgresHelpers import TestgresNodeConnection
from ...TestServices import TestServices
from ...TestCheckRunConditions import TestCheckRunConditions
from ...PgHelper import PgHelper

import os
import pytest
import logging
import typing

# //////////////////////////////////////////////////////////////////////////////
# TestSet001__MultiXacts__HELPER


class TestSet001__MultiXacts__HELPER:
    class tagData004:
        nextMXid: int
        oldestMXid: int
        cMXIDs: int

        # ----------------------------------------------------------------
        def __init__(self, nextMXid: int, oldestMXid: int, cMXIDs: int):
            assert type(nextMXid) == int
            assert type(oldestMXid) == int
            assert type(cMXIDs) == int

            self.nextMXid = nextMXid
            self.oldestMXid = oldestMXid
            self.cMXIDs = cMXIDs

        # ----------------------------------------------------------------
        def sign(self) -> str:
            assert type(self.nextMXid) == int
            assert type(self.oldestMXid) == int
            assert type(self.cMXIDs) == int
            assert self.nextMXid > 0
            assert self.oldestMXid > 0
            assert self.cMXIDs > 0

            return "next_{0}-old_{1}-cmxids_{2}".format(
                self.nextMXid, self.oldestMXid, self.cMXIDs
            )


# //////////////////////////////////////////////////////////////////////////////
# TestSet001__MultiXacts


class TestSet001__MultiXacts(TestSet001__MultiXacts__HELPER):
    C_UINT32_MAX = PgHelper.C_UINT32_MAX
    C_UINT64_MAX = PgHelper.C_UINT64_MAX

    # --------------------------------------------------------------------
    def GenData004() -> list[TestSet001__MultiXacts__HELPER.tagData004]:
        Data_t = TestSet001__MultiXacts__HELPER.tagData004

        C_UINT63_MAX = int(int(PgHelper.C_4GB * PgHelper.C_4GB) // 2) - 1

        result = list[Data_t]()

        result.append(Data_t(100, 100, 100)),
        result.append(Data_t(65000, 65000, 2000)),
        result.append(Data_t(67000, 67000, 2000)),

        xids: list[int] = [
            *range(PgHelper.C_1GB - 4, PgHelper.C_1GB + 5),
            *range(PgHelper.C_2GB - 4, PgHelper.C_2GB + 5),
            *range(PgHelper.C_4GB - 4, PgHelper.C_4GB + 5),
            *range(
                PgHelper.C_4GB + PgHelper.C_1GB - 4, PgHelper.C_4GB + PgHelper.C_1GB + 5
            ),
            *range(C_UINT63_MAX - 13, C_UINT63_MAX + 13),
            *range(PgHelper.C_UINT64_MAX - 8, PgHelper.C_UINT64_MAX + 1),
        ]

        for xid in xids:
            for count in [1, 2, 3, 4, 5]:
                result.append(Data_t(xid, xid, count))

        return result

    # --------------------------------------------------------------------
    sm_Data004: list[TestSet001__MultiXacts__HELPER.tagData004] = GenData004()

    # --------------------------------------------------------------------
    @pytest.fixture(params=sm_Data004, ids=[x.sign() for x in sm_Data004])
    def data004(
        self, request: pytest.FixtureRequest
    ) -> TestSet001__MultiXacts__HELPER.tagData004:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param) == TestSet001__MultiXacts__HELPER.tagData004
        return request.param

    # --------------------------------------------------------------------
    def test_004__multixacts(
        self,
        request: pytest.FixtureRequest,
        data004: TestSet001__MultiXacts__HELPER.tagData004,
    ):
        assert isinstance(request, pytest.FixtureRequest)
        assert type(data004) == TestSet001__MultiXacts__HELPER.tagData004

        oldBinDir = TestServices.GetOldBinDir()
        TestCheckRunConditions.PgResetwalIsRequired(oldBinDir)

        C_DB = "postgres"
        C_CONNECTION_COUNT = 2

        tmpDir = TestServices.GetCurTestTmpDir(request)

        sourceNode: TestgresNode = None
        targetNode: TestgresNode = None

        connections = list[TestgresNodeConnection]()

        try:
            logging.info("Prepare source database ...")

            sourceNode = TestgresHelpers.NODE__make_simple("source", tmpDir, oldBinDir)

            # -------------------------
            TestgresHelpers.NODE__set_multixacts(
                sourceNode,
                data004.nextMXid,
                data004.oldestMXid,
            )

            # -------------------------
            TestgresHelpers.NODE__start(sourceNode)

            # -------------------------
            TestgresHelpers.NODE__psql(
                sourceNode,
                C_DB,
                "CREATE TABLE test_table (id integer NOT NULL PRIMARY KEY, val text);\n"
                + "INSERT INTO test_table VALUES (1, 'a');\n",
            )

            # -------------------------
            logging.info("Connections are creating ...")
            while len(connections) < C_CONNECTION_COUNT:
                cn = sourceNode.connect(dbname=C_DB)
                connections.append(cn)

            # -------------------------
            expectedMXID = data004.nextMXid

            cMXIDs = 0

            MultiXacts = dict[int, set[int]]()

            sourceMXidMembers: typing.Optional[set[int]] = None

            sourceRecordXMax_MXID: typing.Optional[int] = None

            cnPIDs = list[int]()

            while cMXIDs < data004.cMXIDs:
                cMXIDs += 1
                logging.info("--------------------- cMXIDs: {0}".format(cMXIDs))

                logging.info(
                    "MultiTransaction is creating [expected MXID is {0}]...".format(
                        expectedMXID
                    )
                )

                sourceMXidMembers = set[int]()

                sourceRecordXMax_MXID = None

                for iCn in range(len(connections)):
                    logging.info("---------------- connection index is {0}".format(iCn))

                    cn = connections[iCn]
                    assert cn is not None
                    assert type(cn) == TestgresNodeConnection

                    cn.begin()

                    assert iCn <= len(cnPIDs)

                    if iCn == len(cnPIDs):
                        current_pid = int(cn.execute("select pg_backend_pid();")[0][0])
                        logging.info("Current pid is {0}".format(current_pid))

                        cnPIDs.append(current_pid)

                    assert iCn < len(cnPIDs)

                    current_txid = int(cn.execute("select txid_current();")[0][0])
                    logging.info("Current txid is {0}".format(current_txid))

                    sourceMXidMembers.add(current_txid)

                    cn.execute("select * from test_table where id = 1 for share;")

                    logging.info("XMax is getting ...")

                    xmax = int(
                        connections[0].execute(
                            "select xmax from test_table where id=1"
                        )[0][0]
                    )

                    logging.info("record xmax is {0}.".format(xmax))

                    assert xmax is not None

                    if iCn == 0:
                        assert xmax == current_txid
                        assert sourceRecordXMax_MXID is None
                    else:
                        assert iCn > 0
                        assert xmax == expectedMXID

                        sourceRecordXMax_MXID = xmax

                        assert len(sourceMXidMembers) > 1

                        MultiXacts[xmax] = sourceMXidMembers.copy()

                        expectedMXID = PgHelper.IncrementMultiXid(
                            sourceNode.control_data, expectedMXID
                        )

                        recs = connections[0].execute(
                            "select xid from pg_get_multixact_members('{0}')".format(
                                xmax
                            )
                        )
                        assert type(recs) == list
                        assert len(recs) == len(sourceMXidMembers)
                        xids = [int(x[0]) for x in recs]
                        assert set(xids) == sourceMXidMembers
                    continue

                assert sourceRecordXMax_MXID is not None
                assert type(sourceRecordXMax_MXID) == int
                assert sourceRecordXMax_MXID in MultiXacts.keys()

                for mxid in MultiXacts.keys():
                    expectedXIDS = MultiXacts[mxid]

                    assert type(expectedXIDS) == set
                    assert len(expectedXIDS) > 1

                    recs = connections[0].execute(
                        "select xid from pg_get_multixact_members('{0}')".format(mxid)
                    )
                    assert type(recs) == list
                    assert len(recs) == len(expectedXIDS)
                    xids = [int(x[0]) for x in recs]
                    assert set(xids) == expectedXIDS
                    continue

                logging.info("Commits...")
                for cn in connections:
                    cn.commit()

                continue

            logging.info("Connections are closing ...")
            while len(connections) > 0:
                connections.pop().close()
            logging.info("Connections closed.")

            TestgresHelpers.NODE__stop(sourceNode)

            assert sourceRecordXMax_MXID is not None
            assert type(sourceRecordXMax_MXID) == int
            assert sourceMXidMembers is not None
            assert type(sourceMXidMembers) == set
            assert len(sourceMXidMembers) == C_CONNECTION_COUNT

            # ------------------------------------
            logging.info("Make target database")

            targetNode = TestgresHelpers.NODE__make_simple(
                "target", tmpDir, TestServices.GetNewBinDir()
            )

            # ------------------------------------
            TestgresHelpers.NODE__utility(
                sourceNode,
                [
                    os.path.join(sourceNode.bin_dir, "pg_controldata"),
                    sourceNode.data_dir,
                ],
            )

            # ------------------------------------
            TestgresHelpers.NODE__upgrade_from(targetNode, sourceNode)

            # ------------------------------------
            TestgresHelpers.NODE__utility(
                targetNode,
                [
                    os.path.join(targetNode.bin_dir, "pg_controldata"),
                    targetNode.data_dir,
                ],
            )

            # ------------------------------------
            TestgresHelpers.NODE__start(targetNode)

            # ------------------------------------
            cn = targetNode.connect(dbname=C_DB)

            correctSourceMXID = PgHelper.GetMultiXidConverter(
                sourceNode.control_data, targetNode.control_data
            )

            assert correctSourceMXID is not None

            try:
                logging.info("XMax of record in target cluster is checking...")

                targetRecordXMax_MXID: int = int(
                    cn.execute("select xmax from test_table where id=1")[0][0]
                )

                assert type(targetRecordXMax_MXID) == int

                logging.info(
                    "XMax of target record is {0}.".format(targetRecordXMax_MXID)
                )

                sourceRecordXMax_MXID_c = correctSourceMXID(
                    sourceRecordXMax_MXID, data004.oldestMXid
                )

                assert targetRecordXMax_MXID == sourceRecordXMax_MXID_c

                recs = cn.execute(
                    "select xid from pg_get_multixact_members('{0}')".format(
                        targetRecordXMax_MXID
                    )
                )

                targetRecordXMax_MXID_v = [int(x[0]) for x in recs]

                # assert len(targetRecordXMax_MXID_v) == C_CONNECTION_COUNT

                targetRecordXMax_MXID_s = set[int](targetRecordXMax_MXID_v)

                assert targetRecordXMax_MXID_s == sourceMXidMembers
            finally:
                cn.close()

            # ------------------------------------
            logging.info("Do VACUUM ANALYZE through connection ...")

            cn = targetNode.connect(dbname=C_DB)

            try:
                cn.connection.autocommit = True

                pid = cn.execute("select pg_backend_pid();")[0][0]

                logging.info("target backend pid is {0}".format(pid))

                cn.execute("VACUUM ANALYZE;")
            finally:
                cn.close()

            # ------------------------------------
            r = TestgresHelpers.NODE__psql(targetNode, "postgres", "VACUUM ANALYZE;")

            assert r == ""

            # ------------------------------------
            TestgresHelpers.NODE__utility(
                targetNode,
                [
                    os.path.join(targetNode.bin_dir, "vacuumdb"),
                    "-h",
                    targetNode.host,
                    "-p",
                    str(targetNode.port),
                    "--all",
                    "--analyze-in-stages",
                ],
            )

        finally:
            while len(connections) > 0:
                connections.pop().close()

            TestgresHelpers.NODE__safe_stop_before_exit(sourceNode)
            TestgresHelpers.NODE__safe_stop_before_exit(targetNode)

        TestgresHelpers.NODE__safe_cleanup(sourceNode)
        TestgresHelpers.NODE__safe_cleanup(targetNode)
        return


# //////////////////////////////////////////////////////////////////////////////
