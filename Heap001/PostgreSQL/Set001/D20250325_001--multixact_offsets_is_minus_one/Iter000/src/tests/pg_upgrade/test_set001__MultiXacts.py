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
    # --------------------------------------------------------------------
    class tagData006:
        nextMultiXactOffset: int
        cItems: int
        cMXIDs: int

        # ----------------------------------------------------------------
        def __init__(self, nextMultiXactOffset: int, cItems: int, cMXIDs: int):
            assert type(nextMultiXactOffset) == int
            assert type(cItems) == int
            assert type(cMXIDs) == int

            assert cItems == 0 or cItems > 1

            self.nextMultiXactOffset = nextMultiXactOffset
            self.cItems = cItems
            self.cMXIDs = cMXIDs

        # ----------------------------------------------------------------
        def sign(self) -> str:
            assert type(self.nextMultiXactOffset) == int
            assert type(self.cItems) == int
            assert type(self.cMXIDs) == int
            assert self.nextMultiXactOffset >= 0
            assert self.cItems == 0 or self.cItems > 1
            assert self.cMXIDs >= 0

            return "next_{0}-citems_{1}-cmxids_{2}".format(
                self.nextMultiXactOffset, self.cItems, self.cMXIDs
            )


# //////////////////////////////////////////////////////////////////////////////
# TestSet001__MultiXacts


class TestSet001__MultiXacts(TestSet001__MultiXacts__HELPER):
    C_UINT32_MAX = PgHelper.C_UINT32_MAX
    C_UINT64_MAX = PgHelper.C_UINT64_MAX

    # --------------------------------------------------------------------
    def Data006Generator() -> list[TestSet001__MultiXacts__HELPER.tagData006]:
        r = list[TestSet001__MultiXacts__HELPER.tagData006]()

        r.append(TestSet001__MultiXacts__HELPER.tagData006(1, 2, 1))
        r.append(TestSet001__MultiXacts__HELPER.tagData006(100, 100, 100))
        r.append(TestSet001__MultiXacts__HELPER.tagData006(65000, 3, 2000))
        r.append(TestSet001__MultiXacts__HELPER.tagData006(67000, 3, 2000))

        C_64KB = 64 * 1024

        C_OFFSET_MULTIPLICATOR = 52352  # 0xCC80

        C_MAX_OFFSET = C_64KB * C_OFFSET_MULTIPLICATOR - 1

        nexts = [
            0,
            C_OFFSET_MULTIPLICATOR - 2,
            (2 * C_OFFSET_MULTIPLICATOR) - 2,
            1000000000,
            2147483647,
            2147483648,
            2147483649,
            C_MAX_OFFSET - C_64KB - 2,
            C_MAX_OFFSET - C_64KB - 1,
            C_MAX_OFFSET - C_64KB - 0,
            C_MAX_OFFSET - C_64KB + 1,
            C_MAX_OFFSET - C_64KB + 2,
            C_MAX_OFFSET - 1000,
            *range(C_MAX_OFFSET - 20, C_MAX_OFFSET),
            C_MAX_OFFSET,
            *range(C_MAX_OFFSET + 1, C_MAX_OFFSET + 20 + 1),
            *range(PgHelper.C_4GB - 20, PgHelper.C_4GB + 21),
            *range(
                PgHelper.C_4GB + PgHelper.C_1GB - 20,
                PgHelper.C_4GB + PgHelper.C_1GB + 21,
            ),
            *range(PgHelper.C_UINT64_MAX - 20, PgHelper.C_UINT64_MAX + 1),
        ]

        for x1 in nexts:
            r.append(TestSet001__MultiXacts__HELPER.tagData006(x1, 0, 0))

            #
            # 2..13. Each OLD multixact_member block contains 4 elements.
            #
            for x2 in [*range(2, 13), 100]:
                for x3 in [*range(1, 5), 100]:
                    r.append(TestSet001__MultiXacts__HELPER.tagData006(x1, x2, x3))

        return r

    # --------------------------------------------------------------------
    sm_Data006: list[TestSet001__MultiXacts__HELPER.tagData006] = Data006Generator()

    # --------------------------------------------------------------------
    @pytest.fixture(params=sm_Data006, ids=[x.sign() for x in sm_Data006])
    def data006(
        self, request: pytest.FixtureRequest
    ) -> TestSet001__MultiXacts__HELPER.tagData006:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param) == TestSet001__MultiXacts__HELPER.tagData006
        return request.param

    # --------------------------------------------------------------------
    def test_006__multixact_offsets(
        self,
        request: pytest.FixtureRequest,
        data006: TestSet001__MultiXacts__HELPER.tagData006,
    ):
        assert isinstance(request, pytest.FixtureRequest)
        assert type(data006) == __class__.tagData006

        oldBinDir = TestServices.GetOldBinDir()
        TestCheckRunConditions.PgResetwalIsRequired(oldBinDir)

        C_DB = "postgres"
        C_CONNECTION_COUNT = data006.cItems

        C_MIN_MXID = 1
        C_MAX_MXID = 4294967295

        assert isinstance(request, pytest.FixtureRequest)

        tmpDir = TestServices.GetCurTestTmpDir(request)

        sourceNode: TestgresNode = None
        targetNode: TestgresNode = None

        connections = list[TestgresNodeConnection]()

        try:
            logging.info("Prepare source database ...")

            sourceNode = TestgresHelpers.NODE__make_simple("source", tmpDir, oldBinDir)

            # -------------------------
            TestgresHelpers.NODE__set_multixact_offset(
                sourceNode,
                data006.nextMultiXactOffset,
            )

            # -------------------------
            TestgresHelpers.NODE__start(sourceNode)

            # -------------------------
            TestgresHelpers.NODE__psql(
                sourceNode,
                C_DB,
                "CREATE TABLE test_table (id integer NOT NULL PRIMARY KEY, val text);\n"
                "ALTER TABLE test_table SET (autovacuum_enabled = off);\n"
                + "INSERT INTO test_table VALUES (1, 'a');\n",
            )

            # -------------------------
            logging.info("Connections are creating ...")
            while len(connections) < C_CONNECTION_COUNT:
                cn = sourceNode.connect(dbname=C_DB)
                connections.append(cn)

            # -------------------------
            expectedMXID = 1

            cMXIDs = 0

            MultiXacts = dict[int, set[int]]()

            sourceMXidMembers: typing.Optional[set[int]] = None

            sourceRecordXMax_MXID: typing.Optional[int] = None

            while cMXIDs < data006.cMXIDs:
                assert expectedMXID >= C_MIN_MXID
                assert expectedMXID <= C_MAX_MXID

                cMXIDs += 1
                logging.info("--------------------- cXIDs: {0}".format(cMXIDs))

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

                        if expectedMXID == C_MAX_MXID:
                            expectedMXID = C_MIN_MXID
                        else:
                            expectedMXID += 1

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

                assert (
                    sourceRecordXMax_MXID is None or type(sourceRecordXMax_MXID) == int
                )
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

            assert sourceRecordXMax_MXID is None or type(sourceRecordXMax_MXID) == int

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
            if sourceRecordXMax_MXID is not None:
                type(sourceRecordXMax_MXID) == int

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
                        sourceRecordXMax_MXID, 1
                    )

                    assert targetRecordXMax_MXID == sourceRecordXMax_MXID_c

                    logging.info("Let's check members of known multixacts....")

                    for mxid in MultiXacts.keys():
                        assert type(mxid) == int
                        mxid_c = correctSourceMXID(mxid, 1)

                        logging.info(
                            "Multixact [{0}][old:{1}] is checking ...".format(
                                mxid, mxid_c
                            )
                        )

                        expectedMembers = MultiXacts[mxid]

                        logging.info("Expected members: {0}".format(expectedMembers))

                        recs = cn.execute(
                            "select xid from pg_get_multixact_members('{0}')".format(
                                mxid_c
                            )
                        )

                        targetRecordXMax_MXID_v = [int(x[0]) for x in recs]

                        logging.info(
                            "targetRecordXMax_MXID_v is {0}".format(
                                targetRecordXMax_MXID_v
                            )
                        )

                        assert len(targetRecordXMax_MXID_v) == len(expectedMembers)

                        targetRecordXMax_MXID = set[int](targetRecordXMax_MXID_v)

                        assert targetRecordXMax_MXID == expectedMembers
                        continue
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
