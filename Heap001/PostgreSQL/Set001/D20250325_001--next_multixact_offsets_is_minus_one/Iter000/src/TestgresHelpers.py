# /////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

from __future__ import annotations

from .PgHelper import PgHelper
from .PgHelper import PgControlData

import testgres
import logging
import subprocess
import os
import typing
import pytest

# /////////////////////////////////////////////////////////////////////////////

TestgresNodeConnection = testgres.NodeConnection

# /////////////////////////////////////////////////////////////////////////////
# TestgresNode


class TestgresNode:
    m_InternalNode: testgres.PostgresNode
    m_ControlData: typing.Optional[PgControlData]

    # ---------------------------------------------------------------------
    def __init__(self, internalNode: testgres.PostgresNode):
        assert internalNode is not None
        assert type(internalNode) == testgres.PostgresNode

        self.m_InternalNode = internalNode
        self.m_ControlData = None

    # ---------------------------------------------------------------------
    def create(internalNode: testgres.PostgresNode) -> TestgresNode:
        assert internalNode is not None
        assert type(internalNode) == testgres.PostgresNode

        node = TestgresNode(internalNode)
        return node

    # ---------------------------------------------------------------------
    @property
    def name(self) -> str:
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        r = self.m_InternalNode.name
        assert type(r) == str
        return r

    # ---------------------------------------------------------------------
    @property
    def host(self) -> str:
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        r = self.m_InternalNode.host
        assert type(r) == str
        return r

    # ---------------------------------------------------------------------
    @property
    def port(self) -> int:
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        r = self.m_InternalNode.port
        assert type(r) == int
        return r

    # ---------------------------------------------------------------------
    @property
    def data_dir(self) -> str:
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        r = self.m_InternalNode.data_dir
        assert type(r) == str
        return r

    # ---------------------------------------------------------------------
    @property
    def bin_dir(self) -> str:
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        r = self.m_InternalNode.bin_dir
        assert type(r) == str
        return r

    # ---------------------------------------------------------------------
    @property
    def control_data(self) -> PgControlData:
        assert self.m_ControlData is not None
        assert type(self.m_ControlData) == PgControlData

        return self.m_ControlData

    # ---------------------------------------------------------------------
    @property
    def is_started(self) -> bool:
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        r = self.m_InternalNode.is_started
        assert type(r) == bool
        return r

    # ---------------------------------------------------------------------
    def make_simple(self):
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode

        self.m_InternalNode.init()

        logging.info(
            "Node [{0}] has data_dir: {1}.".format(
                self.m_InternalNode.name, self.m_InternalNode.data_dir
            )
        )

        self.m_InternalNode.set_auto_conf({"autovacuum": False})

        controlData = PgControlData()

        controlData.PageSize = 8 * 1024  # TODO: Get it in runtime!

        controlData.XidSize = PgHelper.DetectXidSize(self.m_InternalNode.data_dir)

        logging.info(
            "Node [{0}] uses xid with size {1}.".format(
                self.m_InternalNode.name, controlData.XidSize
            )
        )

        self.m_ControlData = controlData
        return

    # ---------------------------------------------------------------------
    def start(self):
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        self.m_InternalNode.start()

    # ---------------------------------------------------------------------
    def stop(self):
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        self.m_InternalNode.stop()

    # ---------------------------------------------------------------------
    def cleanup(self):
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        self.m_InternalNode.cleanup()
        self.m_XidSize = None

    # ---------------------------------------------------------------------
    def free_port(self):
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        self.m_InternalNode.free_port()

    # ---------------------------------------------------------------------
    def connect(self, dbname: str) -> TestgresNodeConnection:
        assert self.m_InternalNode is not None
        assert type(self.m_InternalNode) == testgres.PostgresNode
        return self.m_InternalNode.connect(dbname=dbname)


# /////////////////////////////////////////////////////////////////////////////
# TestgresHelpers


class TestgresHelpers:
    C_LD_LIBRARY_PATH = "LD_LIBRARY_PATH"

    # C_DEFAULT_PG_PORT = 5432

    # --------------------------------------------------------------------
    def NODE__make_simple(nodeName: str, baseDir: str, binDir: str) -> TestgresNode:
        assert nodeName is not None
        assert baseDir is not None
        assert binDir is not None
        assert type(nodeName) == str
        assert type(baseDir) == str
        assert type(binDir) == str

        logging.info("Node [{0}] is creating [binDir: {1}]...".format(nodeName, binDir))

        nodeBaseDir = os.path.join(baseDir, nodeName)
        assert type(nodeBaseDir) == str

        internalNode = testgres.PostgresNode(
            nodeName,
            base_dir=nodeBaseDir,
            bin_dir=binDir,
            # port=__class__.C_DEFAULT_PG_PORT,
        )

        # cmdParams = [
        #    os.path.join(node.bin_dir, "initdb"),
        #    "-k",
        #    "-D",
        #    node.data_dir,
        # ]

        # r = __class__.Helper__ExecPgUtility(node.bin_dir, cmdParams)

        # __class__.Helper__LogOutputAndCheckExecResult(cmdParams, r)

        r: TestgresNode = TestgresNode.create(internalNode)

        assert r is not None
        assert type(r) == TestgresNode

        r.make_simple()

        return r

    # --------------------------------------------------------------------
    def NODE__start(node: TestgresNode):
        assert node is not None
        assert type(node) == TestgresNode

        logging.info("Node [{0}] is starting...".format(node.name))

        node.start()

    # --------------------------------------------------------------------
    def NODE__stop(node: testgres.PostgresNode):
        assert node is not None
        assert type(node) == TestgresNode

        logging.info("Node [{0}] is stopping...".format(node.name))
        node.stop()

    # --------------------------------------------------------------------
    def NODE__safe_stop_before_exit(node: testgres.PostgresNode):
        if node is None:
            return

        assert type(node) == TestgresNode
        assert type(node.is_started) == bool

        if node.is_started:
            logging.info("Node [{0}] is stopping...".format(node.name))
            node.stop()

        logging.info("Node [{0}] releases its port ...".format(node.name))
        node.free_port()

    # --------------------------------------------------------------------
    def NODE__safe_cleanup(node: testgres.PostgresNode):
        if node is None:
            return

        assert node is not None
        assert type(node) == TestgresNode

        logging.info("Node [{0}] is cleaning...".format(node.name))
        node.cleanup()
        node.free_port()

    # ---------------------------------------------------------------------
    def NODE__set_multixacts(
        node: TestgresNode, nextMultiXact: int, oldestMultiXact: int
    ):
        assert node is not None
        assert nextMultiXact is not None
        assert oldestMultiXact is not None
        assert type(node) == TestgresNode
        assert type(nextMultiXact) == int
        assert type(oldestMultiXact) == int
        assert nextMultiXact >= 0
        assert oldestMultiXact >= 0

        # -------------------------
        maxMultiXid = PgHelper.GetMaxMuiltiXid(node.control_data)

        if nextMultiXact > maxMultiXid:
            TestgresHelpers.NODE__safe_cleanup(node)
            pytest.skip(
                "NextMutiXact {0} is not supported by node [{1}]. MaxMultiXid is [{2}].".format(
                    nextMultiXact, node.name, maxMultiXid
                )
            )

        if oldestMultiXact > maxMultiXid:
            TestgresHelpers.NODE__safe_cleanup(node)
            pytest.skip(
                "OldestMutiXact {0} is not supported by node [{1}]. MaxMultiXid is [{2}].".format(
                    oldestMultiXact, node.name, maxMultiXid
                )
            )

        # -------------------------
        TestgresHelpers.NODE__resetwal(
            node,
            multixid=(nextMultiXact, oldestMultiXact),
        )

        # -------------------------
        offsetSegmentFileName = PgHelper.MakeMultixactOffsetSegmentFileName(
            node.m_ControlData, nextMultiXact
        )

        logging.info(
            "Run bash to create pg_multixact/offsets file ({0}) ...".format(
                offsetSegmentFileName
            )
        )

        offsetSegmentFileSize = PgHelper.MakeMultixactOffsetSegmentSize(
            node.m_ControlData
        )

        xactFilesGeneratorScript = (
            "dd if=/dev/zero of={0}/pg_multixact/offsets/{1} bs={2} count=1".format(
                node.data_dir, offsetSegmentFileName, offsetSegmentFileSize
            )
        )

        bashCmd = ["bash", "-c", xactFilesGeneratorScript]

        TestgresHelpers.ExecCmd(bashCmd)

    # ---------------------------------------------------------------------
    def NODE__set_xacts(node: TestgresNode, nextXID: int, oldestXID: int):
        assert node is not None
        assert nextXID is not None
        assert oldestXID is not None
        assert type(node) == TestgresNode
        assert type(nextXID) == int
        assert type(oldestXID) == int
        assert nextXID >= 0
        assert oldestXID >= 0

        # -------------------------
        maxXid = PgHelper.GetMaxXid(node.control_data)

        if nextXID > maxXid:
            TestgresHelpers.NODE__safe_cleanup(node)
            pytest.skip(
                "NextXID {0} is not supported by node [{1}]. MaxXid is [{2}].".format(
                    nextXID, node.name, maxXid
                )
            )

        if oldestXID > maxXid:
            TestgresHelpers.NODE__safe_cleanup(node)
            pytest.skip(
                "OldestXID {0} is not supported by node [{1}]. MaxXid is [{2}].".format(
                    nextXID, node.name, maxXid
                )
            )

        # -------------------------
        __class__.NODE__resetwal(node, next_xid=nextXID, oldest_xid=oldestXID)

        # -------------------------
        segmentSize = PgHelper.GetXidSegmentSize(node.control_data)

        segmentNo_oldestXID = PgHelper.CalcXidSegmentNo(node.control_data, oldestXID)
        segmentNo_nextXID = PgHelper.CalcXidSegmentNo(node.control_data, nextXID)

        segmentNo = segmentNo_oldestXID

        while True:
            segmentFileName = PgHelper.GetXactSegmentFileName(
                node.control_data, segmentNo
            )


            segmentFilePath = os.path.join(node.data_dir, "pg_xact", segmentFileName)

            if not os.path.exists(segmentFilePath):
                # let's create a new segment (dd is used just for fun)
                xactFilesGeneratorScript = (
                    "dd if=/dev/zero of={0} bs={1} count=1".format(
                        segmentFilePath, segmentSize
                    )
                )

                bashCmd = ["bash", "-c", xactFilesGeneratorScript]

                TestgresHelpers.ExecCmd(bashCmd)
            else:
                # File exist

                # TODO: check it size?
                fs = os.path.getsize(segmentFilePath)

                if fs < segmentSize:
                    xactFilesGeneratorScript = (
                        "dd if=/dev/zero of={0} bs={1} count=1 seek={2}".format(
                            segmentFilePath, segmentSize - fs, fs
                        )
                    )

                    bashCmd = ["bash", "-c", xactFilesGeneratorScript]

                    TestgresHelpers.ExecCmd(bashCmd)
                pass

            if segmentNo == segmentNo_nextXID:
                break

            segmentNo = PgHelper.GetNextXactSegmentNo(node.control_data, segmentNo)

    # ---------------------------------------------------------------------
    def NODE__set_multixact_offset(node: TestgresNode, nextMultiXactOffset: int):
        assert node is not None
        assert nextMultiXactOffset is not None
        assert type(node) == TestgresNode
        assert type(nextMultiXactOffset) == int

        # -------------------------
        maxSourceMultiXactMemberOffset = PgHelper.GetMaxMuiltiXactMemberOffset(
            node.control_data
        )

        if nextMultiXactOffset > maxSourceMultiXactMemberOffset:
            TestgresHelpers.NODE__safe_cleanup(node)
            pytest.skip(
                "NextMutiXactOffset {0} is not supported by node [{1}]. MaxSourceMultiXactMemberOffset is [{2}].".format(
                    nextMultiXactOffset, node.name, maxSourceMultiXactMemberOffset
                )
            )

        # -------------------------
        __class__.NODE__resetwal(node, multixact_offset=nextMultiXactOffset)

        # -------------------------
        memberSegmentFileName = PgHelper.MakeMultixactMemberSegmentFileName(
            node.control_data, nextMultiXactOffset
        )

        assert type(memberSegmentFileName) == str

        logging.info(
            "Run bash to create pg_multixact/members file ({0}) ...".format(
                memberSegmentFileName
            )
        )

        memberSegmentFileSize = PgHelper.MakeMultixactMemberSegmentSize(
            node.m_ControlData
        )
        xactFilesGeneratorScript = (
            "dd if=/dev/zero of={0}/pg_multixact/members/{1} bs={2} count=1".format(
                node.data_dir, memberSegmentFileName, memberSegmentFileSize
            )
        )

        bashCmd = ["bash", "-c", xactFilesGeneratorScript]

        TestgresHelpers.ExecCmd(bashCmd)

    # ---------------------------------------------------------------------
    def NODE__psql(node: testgres.PostgresNode, dbname: str, query: str) -> str:
        assert node is not None
        assert dbname is not None
        assert query is not None
        assert type(node) == TestgresNode
        assert type(dbname) == str
        assert type(query) == str

        logging.info(
            "Run PSQL [node {0}][dbname {1}] query [{2}] ...".format(
                node.name, dbname, query
            )
        )

        cmdParams = [
            os.path.join(node.bin_dir, "psql"),
            "-h",
            node.host,
            "-p",
            str(node.port),
            "-d",
            dbname,
            "-X",  # no .psqlrc
            "-A",  # unaligned output
            "-t",  # print rows only
            "-q",  # run quietly
            "-c",
            query,
        ]

        r = __class__.Helper__ExecPgUtility(node.bin_dir, cmdParams)

        return __class__.Helper__LogOutputAndCheckExecResult(cmdParams, r)

    # ---------------------------------------------------------------------
    def NODE__resetwal(
        node: testgres.PostgresNode,
        next_xid: typing.Optional[int] = None,
        oldest_xid: typing.Optional[int] = None,
        epoch: typing.Optional[int] = None,
        next_oid: typing.Optional[int] = None,
        multixid: typing.Optional[tuple[int, int]] = None,
        multixact_offset: typing.Optional[int] = None,
    ) -> None:
        assert node is not None

        assert type(node) == TestgresNode

        logging.info("resetwal of node [{0}] ...".format(node.name))

        cmdParams = []

        if next_xid is not None:
            assert type(next_xid) == int
            cmdParams.append("-x")
            cmdParams.append(str(next_xid))

        if oldest_xid is not None:
            assert type(oldest_xid) == int
            cmdParams.append("-u")
            cmdParams.append(str(oldest_xid))

        if next_oid is not None:
            assert type(next_oid) == int
            cmdParams.append("-o")
            cmdParams.append(str(next_oid))

        if epoch is not None:
            assert type(epoch) == int
            cmdParams.append("-e")
            cmdParams.append(str(epoch))

        if multixid is not None:
            assert type(multixid) == tuple
            assert len(multixid) == 2
            assert type(multixid[0]) == int
            assert type(multixid[1]) == int
            cmdParams.append("-m")
            cmdParams.append(str(multixid[0]) + "," + str(multixid[1]))

        if multixact_offset is not None:
            assert type(multixact_offset) == int
            assert multixact_offset >= 0
            cmdParams.append("-O")
            cmdParams.append(str(multixact_offset))

        if len(cmdParams) == 0:
            raise RuntimeError("No any parameters of resetwal are defined.")

        cmdParams.insert(0, os.path.join(node.bin_dir, "pg_resetwal"))
        cmdParams.append("-D")
        cmdParams.append(node.data_dir)

        r = __class__.Helper__ExecPgUtility(node.bin_dir, cmdParams)

        __class__.Helper__LogOutputAndCheckExecResult(cmdParams, r)
        return

    # ---------------------------------------------------------------------
    def NODE__upgrade_from(targetNode: TestgresNode, sourceNode: TestgresNode) -> None:
        assert targetNode is not None
        assert sourceNode is not None

        assert type(targetNode) == TestgresNode
        assert type(sourceNode) == TestgresNode

        logging.info(
            "Upgrade node [{0}] from node [{1}] ...".format(
                targetNode.name, sourceNode.name
            )
        )

        cmdParams = [
            os.path.join(targetNode.bin_dir, "pg_upgrade"),
            "--old-bindir",
            sourceNode.bin_dir,
            "--new-bindir",
            targetNode.bin_dir,
            "--old-datadir",
            sourceNode.data_dir,
            "--new-datadir",
            targetNode.data_dir,
            "--old-port",
            str(sourceNode.port),
            "--new-port",
            str(targetNode.port),
        ]

        r = __class__.Helper__ExecPgUtility(targetNode.bin_dir, cmdParams)

        __class__.Helper__LogOutputAndCheckExecResult(cmdParams, r)
        return

    # ---------------------------------------------------------------------
    def NODE__get_version_string(binDir: TestgresNode) -> str:
        assert binDir is not None
        assert type(binDir) == str

        logging.info("Node binaries version [{0}] is detecting ...".format(binDir))

        cmdParams = [
            os.path.join(binDir, "postgres"),
            "--version",
        ]

        r = __class__.Helper__ExecPgUtility(binDir, cmdParams)

        ver: str = __class__.Helper__LogOutputAndCheckExecResult(cmdParams, r)

        assert type(ver) == str

        assert __class__.Helper__total_strip("") == ""
        assert __class__.Helper__total_strip(" ") == ""
        assert __class__.Helper__total_strip(" a") == "a"
        assert __class__.Helper__total_strip("a ") == "a"
        assert __class__.Helper__total_strip(" a ") == "a"
        assert __class__.Helper__total_strip(" a a ") == "a a"

        ver = __class__.Helper__total_strip(ver)

        r1 = 0
        while r1 < len(ver) and not ver[r1].isdigit():
            r1 += 1

        r2 = r1

        while r2 < len(ver) and (ver[r2].isdigit() or ver[r2] == "."):
            r2 += 1

        ver = ver[r1:r2]

        if ver == "":
            raise RuntimeError("Version string is empty.")

        logging.info("Node binaries version [{0}] is [{1}].".format(binDir, ver))

        return ver

    # ---------------------------------------------------------------------
    def NODE__utility(node: TestgresNode, cmdParams: list[str]) -> str:
        assert node is not None
        assert cmdParams is not None
        assert isinstance(node, TestgresNode)
        assert type(cmdParams) == list

        r = __class__.Helper__ExecPgUtility(node.bin_dir, cmdParams)

        return __class__.Helper__LogOutputAndCheckExecResult(cmdParams, r)

    # ---------------------------------------------------------------------
    def ExecCmd(cmdParams: list) -> str:
        assert cmdParams is not None
        assert type(cmdParams) == list

        r = __class__.Helper__ExecCmd(cmdParams, None)

        return __class__.Helper__LogOutputAndCheckExecResult(cmdParams, r)

    # Helper --------------------------------------------------------------
    def Helper__LogOutputAndCheckExecResult(
        cmdParams: list, execResult: tuple[int, bytes, bytes]
    ) -> str:
        assert type(execResult) == tuple
        assert len(execResult) == 3
        assert type(execResult[0]) == int
        assert type(execResult[1]) == bytes
        assert type(execResult[2]) == bytes
        assert type(cmdParams) == list
        assert len(cmdParams) > 0

        output = execResult[1].decode()
        assert type(output) == str
        logging.info("OUTPUT:\n{0}".format(execResult[1].decode().strip()))

        if execResult[0] == 0:
            return output

        errMsgItems = []

        errMsgItems.append(
            "Utility returns non-zero error code [{0}].".format(execResult[0])
        )

        errMsgItems.append("Command line arguments: {0}.".format(cmdParams))

        if execResult[2] is not None:
            assert type(execResult[2]) == bytes
            errMsgItems.append("Error message is [{0}].".format(execResult[2].decode()))

        errMsg = "\n".join(errMsgItems)

        raise RuntimeError(errMsg)

    # ---------------------------------------------------------------------
    def Helper__ExecPgUtility(binDir: str, cmdParams: list[str]):
        assert binDir is not None
        assert cmdParams is not None
        assert type(binDir) == str
        assert type(cmdParams) == list

        new_LD_LIBRARY_PATH = os.path.realpath(os.path.join(binDir, "..", "lib"))

        if __class__.C_LD_LIBRARY_PATH in os.environ.keys():
            x = os.environ[__class__.C_LD_LIBRARY_PATH]
            new_LD_LIBRARY_PATH = new_LD_LIBRARY_PATH + "::" + x

        newEnv = os.environ.copy()

        newEnv[__class__.C_LD_LIBRARY_PATH] = new_LD_LIBRARY_PATH

        return __class__.Helper__ExecCmd(cmdParams, newEnv)

    # ---------------------------------------------------------------------
    def Helper__ExecCmd(cmdParams: list[str], env: typing.Optional[dict]):
        assert cmdParams is not None
        assert type(cmdParams) == list

        logging.info("Exec command line: {0}".format(cmdParams))

        return __class__.Helper__RunCommand(cmdParams, env)

    # --------------------------------------------------------------------
    def Helper__RunCommand(
        cmd: list, env: typing.Optional[dict]
    ) -> tuple[int, bytes, bytes]:
        assert cmd is not None
        assert type(cmd) == list
        assert env is None or type(env) == dict

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd="/tmp"
        )
        assert not (process is None)
        output, error = process.communicate()

        assert type(output) == bytes  # noqa: E721
        assert type(error) == bytes  # noqa: E721

        return (process.returncode, output, error)

    # ---------------------------------------------------------------------
    def Helper__total_strip(s: str) -> str:
        seps = " \t\r\n"

        b = 0
        while b < len(s) and s[b] in seps:
            b += 1

        e = len(s)
        while b < e and s[e - 1] in seps:
            e -= 1

        r = s[b:e]
        assert len(r) == (e - b)

        return r


# /////////////////////////////////////////////////////////////////////////////
