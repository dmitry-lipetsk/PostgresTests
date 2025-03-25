# /////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

from __future__ import annotations

import os
import typing

# /////////////////////////////////////////////////////////////////////////////
# PgControlData


class PgControlData:
    XidSize: typing.Optional[int]
    PageSize: typing.Optional[int]

    # --------------------------------------------------------------------
    def __init__(self):
        self.XidSize = None
        self.PageSize = None


# /////////////////////////////////////////////////////////////////////////////
# PgXid


class PgXid:
    epoch: int
    number: int

    # --------------------------------------------------------------------
    def __init__(self):
        self.epoch = 0
        self.number = 0

    # --------------------------------------------------------------------
    def create(number: int) -> PgXid:
        assert type(number) == int
        assert number >= 0

        x = PgXid()
        x.number = number

        assert x.epoch == 0

        return x

    # --------------------------------------------------------------------
    def create_ex(epoch: int, number: int) -> PgXid:
        assert type(epoch) == int
        assert epoch >= 0
        assert type(number) == int
        assert number >= 0

        x = PgXid()
        x.epoch = epoch
        x.number = number

        return x

    # --------------------------------------------------------------------
    def copy(self) -> PgXid:
        x = PgXid()

        x.epoch = self.epoch
        x.number = self.number
        return x


# /////////////////////////////////////////////////////////////////////////////
# PgHelper


class PgHelper:
    C_64KB = int(64 * 1024)
    C_1MB = int(1024 * 1024)
    C_2MB = 2 * C_1MB
    C_4MB = 4 * C_1MB
    C_1GB = int(1024 * 1024 * 1024)
    C_2GB = int(2 * C_1GB)
    C_3GB = int(3 * C_1GB)
    C_4GB = int(4 * C_1GB)

    C_UINT32_MAX = C_4GB - 1

    C_UINT64_MAX = int(C_4GB * C_4GB) - 1

    # --------------------------------------------------------------------
    def MakeMultixactOffsetSegmentSize(controlData: PgControlData) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int
        assert type(controlData.PageSize) == int

        if controlData.XidSize == 4:
            cPagesPerSegment = 32
            segmentSize = int(controlData.PageSize * cPagesPerSegment)
            return segmentSize

        if controlData.XidSize == 8:
            cPagesPerSegment = 2048
            segmentSize = int(controlData.PageSize * cPagesPerSegment)
            return segmentSize

        raise RuntimeError("Unknown multixidSize [{0}]".format(controlData.XidSize))

    # --------------------------------------------------------------------
    def MakeMultixactOffsetSegmentFileName(
        controlData: PgControlData, multixid: int
    ) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int
        assert type(controlData.PageSize) == int

        if controlData.XidSize == 4:
            assert multixid >= 0
            assert multixid <= __class__.C_UINT32_MAX

            cPagesPerSegment = 32
            segmentSize = int(controlData.PageSize * cPagesPerSegment)

            cXidPerSegment = int(segmentSize // controlData.XidSize)

            segmentNo = int(multixid // cXidPerSegment)

            r = "{0:04X}".format(segmentNo)
            assert type(r) == str

            return r

        if controlData.XidSize == 8:
            assert multixid >= 0
            assert multixid <= __class__.C_UINT64_MAX

            cPagesPerSegment = 2048
            segmentSize = int(controlData.PageSize * cPagesPerSegment)

            cXidPerSegment = int(segmentSize // controlData.XidSize)

            segmentNo = int(multixid // cXidPerSegment)

            r = "{0:015X}".format(segmentNo)
            assert type(r) == str

            return r

        raise RuntimeError("Unknown multixidSize [{0}]".format(controlData.XidSize))

    # --------------------------------------------------------------------
    def GetMaxMuiltiXid(controlData: PgControlData) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int

        if controlData.XidSize == 4:
            return __class__.C_UINT32_MAX

        if controlData.XidSize == 8:
            return __class__.C_UINT64_MAX

        raise RuntimeError(
            "Can't detect max multixid for size {0}".format(controlData.XidSize)
        )

    # --------------------------------------------------------------------
    def GetMaxXid(controlData: PgControlData) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int

        if controlData.XidSize == 4:
            return __class__.C_UINT32_MAX

        if controlData.XidSize == 8:
            return __class__.C_UINT64_MAX

        raise RuntimeError(
            "Can't detect max xid for size {0}".format(controlData.XidSize)
        )

    # --------------------------------------------------------------------
    def MakeMultixactMemberSegmentSize(controlData: PgControlData) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int
        assert type(controlData.PageSize) == int

        if controlData.XidSize == 4:
            cPagesPerSegment = 32
            segmentSize = int(controlData.PageSize * cPagesPerSegment)
            return segmentSize

        if controlData.XidSize == 8:
            cPagesPerSegment = 2048
            segmentSize = int(controlData.PageSize * cPagesPerSegment)
            return segmentSize

        raise RuntimeError("Unknown multixidSize [{0}]".format(controlData.XidSize))

    # --------------------------------------------------------------------
    def MakeMultixactMemberSegmentFileName(
        controlData: PgControlData, memberOffset: int
    ) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int
        assert type(controlData.PageSize) == int

        if controlData.XidSize == 4:
            assert memberOffset >= 0
            assert memberOffset <= __class__.C_UINT32_MAX

            cPagesPerSegment = 32

            memberSize = 4 + 1
            memberInGroup = 4
            groupSize = memberInGroup * memberSize

            groupsPerPage = int(controlData.PageSize // groupSize)

            membersPerPage = int(memberInGroup * groupsPerPage)

            membersPerSegment = int(cPagesPerSegment * membersPerPage)

            assert membersPerSegment == int(52352)

            segmentNo = int(memberOffset // membersPerSegment)

            r = "{0:04X}".format(segmentNo)
            assert type(r) == str

            return r

        if controlData.XidSize == 8:
            assert memberOffset >= 0
            assert memberOffset <= __class__.C_UINT64_MAX

            cPagesPerSegment = 2048

            memberSize = 8 + 1
            memberInGroup = 8
            groupSize = memberInGroup * memberSize

            groupsPerPage = int(controlData.PageSize // groupSize)

            membersPerPage = int(memberInGroup * groupsPerPage)

            membersPerSegment = int(cPagesPerSegment * membersPerPage)

            segmentNo = int(memberOffset // membersPerSegment)

            r = "{0:015X}".format(segmentNo)
            assert type(r) == str

            return r

        raise RuntimeError("Unknown multixidSize [{0}]".format(controlData.XidSize))

    # --------------------------------------------------------------------
    def GetMaxMuiltiXactMemberOffset(controlData: PgControlData) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int

        if controlData.XidSize == 4:
            return __class__.C_UINT32_MAX

        if controlData.XidSize == 8:
            return __class__.C_UINT64_MAX

        raise RuntimeError(
            "Can't detect max multixact-offset for size {0}".format(controlData.XidSize)
        )

    # --------------------------------------------------------------------
    def GetXactSegmentFileName(controlData: PgControlData, segmentNo: int) -> int:
        assert controlData is not None
        assert segmentNo is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int
        assert type(segmentNo) == int

        if controlData.XidSize == 4:
            assert segmentNo >= 0
            assert segmentNo <= __class__.C_UINT32_MAX // (
                4 * 32 * controlData.PageSize
            )

            r = "{0:04X}".format(segmentNo)
            assert type(r) == str

            return r

        if controlData.XidSize == 8:
            assert segmentNo >= 0
            assert segmentNo <= __class__.C_UINT64_MAX // (
                4 * 2048 * controlData.PageSize
            )

            r = "{0:015X}".format(segmentNo)
            assert type(r) == str

            return r

        raise NotImplementedError(
            "PgHelper::GetXactSegmentFileName is not implemented."
        )

    # ---------------------------------------------------------------------
    def GetNextXactSegmentNo(controlData: PgControlData, segmentNo: int) -> int:
        assert controlData is not None
        assert segmentNo is not None
        assert type(controlData) == PgControlData
        assert type(controlData.XidSize) == int
        assert type(segmentNo) == int

        if controlData.XidSize == 4:
            assert segmentNo >= 0

            cMaxSegmentNo = __class__.C_UINT32_MAX // (
                4 * 32 * controlData.PageSize
            )
            assert segmentNo <= cMaxSegmentNo

            if segmentNo == cMaxSegmentNo:
                return 0

            return segmentNo + 1

        if controlData.XidSize == 8:
            assert segmentNo >= 0

            cMaxSegmentNo = __class__.C_UINT64_MAX // (
                4 * 2048 * controlData.PageSize
            )
            assert segmentNo <= cMaxSegmentNo

            if segmentNo == cMaxSegmentNo:
                return 0

            return segmentNo + 1

        raise NotImplementedError("PgHelper::GetNextXactSegmentNo is not implemented.")

    # ---------------------------------------------------------------------
    def GetXidSegmentSize(controlData: PgControlData) -> int:
        assert controlData is not None
        assert type(controlData) == PgControlData

        if controlData.XidSize == 4:
            return 32 * controlData.PageSize

        if controlData.XidSize == 8:
            return 2048 * controlData.PageSize

        raise NotImplementedError("PgHelper::GetXidSegmentSize is not implemented.")

    # ---------------------------------------------------------------------
    def CalcXidSegmentNo(controlData: PgControlData, xid: int) -> int:
        assert controlData is not None
        assert xid is not None
        assert type(controlData) == PgControlData
        assert type(xid) == int

        if controlData.XidSize == 4:
            assert xid >= 3
            assert xid <= PgHelper.C_UINT32_MAX

            segmentSize = 32 * controlData.PageSize

            x1 = xid // 4
            x2 = x1 // segmentSize
            return x2

        if controlData.XidSize == 8:
            assert xid >= 3
            assert xid <= PgHelper.C_UINT64_MAX

            segmentSize = 2048 * controlData.PageSize

            x1 = xid // 4
            x2 = x1 // segmentSize
            return x2

        raise NotImplementedError("PgHelper::CalcXidSegmentNo is not implemented.")

    # --------------------------------------------------------------------
    def DetectXidSize(dataDir: str) -> int:
        assert type(dataDir) == str
        assert dataDir != ""

        return __class__.Helper__DetectXidSize(dataDir)

    # --------------------------------------------------------------------
    def IncrementMultiXid(controlData: PgControlData, multixid: int) -> int:
        assert type(controlData) == PgControlData
        assert type(multixid) == int

        if controlData.XidSize == 4:
            assert multixid >= 1
            assert multixid <= __class__.C_UINT32_MAX

            if multixid == __class__.C_UINT32_MAX:
                return 1

            return multixid + 1

        if controlData.XidSize == 8:
            assert multixid >= 1
            assert multixid <= __class__.C_UINT64_MAX

            if multixid == __class__.C_UINT64_MAX:
                return 1

            return multixid + 1

        raise RuntimeError(
            "Can't increment multixid for size {0}".format(controlData.XidSize)
        )

    # --------------------------------------------------------------------
    def IncrementXid(controlData: PgControlData, xid: PgXid) -> PgXid:
        assert controlData is not None
        assert xid is not None
        assert type(controlData) == PgControlData
        assert type(xid) == PgXid

        if controlData.XidSize == 4:
            assert xid.number >= 3
            assert xid.number <= __class__.C_UINT32_MAX
            assert xid.epoch >= 0

            if xid.number == __class__.C_UINT32_MAX:
                return PgXid.create_ex(xid.epoch + 1, 3)

            return PgXid.create_ex(xid.epoch, xid.number + 1)

        if controlData.XidSize == 8:
            assert xid.number >= 1
            assert xid.number <= __class__.C_UINT64_MAX
            assert xid.epoch == 0

            if xid.number == __class__.C_UINT64_MAX:
                return PgXid.create(3)

            return PgXid.create_ex(xid.epoch, xid.number + 1)

        raise RuntimeError(
            "Can't increment xid for size {0}".format(controlData.XidSize)
        )

    # --------------------------------------------------------------------
    def GetFlatXid(controlData: PgControlData, xid: PgXid) -> int:
        assert type(controlData) == PgControlData
        assert type(xid) == PgXid
        assert type(xid.epoch) == int
        assert type(xid.number) == int

        if controlData.XidSize == 4:
            assert xid.epoch >= 0
            assert xid.epoch <= __class__.C_UINT32_MAX
            assert xid.number >= 0
            assert xid.number <= __class__.C_UINT32_MAX

            r = xid.epoch * __class__.C_4GB + xid.number

            assert r >= 0
            assert r <= __class__.C_UINT64_MAX
            return r

        if controlData.XidSize == 8:
            assert xid.epoch == 0
            assert xid.number >= 0
            assert xid.number <= __class__.C_UINT64_MAX

            if xid.number < __class__.C_4GB * __class__.C_2GB:
                return xid.number

            r = xid.number - (__class__.C_4GB * __class__.C_4GB)

            return r

        raise RuntimeError(
            "Can't make a flat xid for size {0}".format(controlData.XidSize)
        )

    # --------------------------------------------------------------------
    def GetMultiXidConverter(
        sourceControlData: PgControlData, targetControlData: PgControlData
    ) -> any:
        assert type(sourceControlData) == PgControlData
        assert type(targetControlData) == PgControlData
        assert type(sourceControlData.XidSize) == int
        assert type(targetControlData.XidSize) == int

        if sourceControlData.XidSize == 4 and targetControlData.XidSize == 4:
            return __class__.CorrectMultiXid32ToMultiXid32

        if sourceControlData.XidSize == 4 and targetControlData.XidSize == 8:
            return __class__.CorrectMultiXid32ToMultiXid64

        if sourceControlData.XidSize == 8 and targetControlData.XidSize == 8:
            return __class__.CorrectMultiXid64ToMultiXid64

        raise RuntimeError(
            "Can't detect a MXid converter. Source MXID size is {0}. Target MXID size is {1}.".format(
                sourceControlData.XidSize, targetControlData.XidSize
            )
        )

    # --------------------------------------------------------------------
    def CorrectMultiXid32ToMultiXid32(mxid32: int, oldestMXid: int) -> int:
        assert type(mxid32) == int
        assert type(oldestMXid) == int
        assert mxid32 >= 1
        assert mxid32 <= __class__.C_UINT32_MAX
        assert oldestMXid >= 1
        assert oldestMXid <= __class__.C_UINT32_MAX

        return mxid32

    # --------------------------------------------------------------------
    def CorrectMultiXid32ToMultiXid64(mxid32: int, oldestMXid: int) -> int:
        assert type(mxid32) == int
        assert type(oldestMXid) == int
        assert mxid32 >= 1
        assert mxid32 <= __class__.C_UINT32_MAX
        assert oldestMXid >= 1
        assert oldestMXid <= __class__.C_UINT32_MAX

        if oldestMXid <= mxid32:
            return mxid32

        assert mxid32 >= 1  # one more time

        assert __class__.C_4GB == __class__.C_UINT32_MAX + 1

        return (mxid32 - 1) + __class__.C_4GB

    # --------------------------------------------------------------------
    def CorrectMultiXid64ToMultiXid64(mxid64: int, oldestMXid: int) -> int:
        assert type(mxid64) == int
        assert type(oldestMXid) == int
        assert mxid64 >= 1
        assert mxid64 <= __class__.C_UINT64_MAX
        assert oldestMXid >= 1
        assert oldestMXid <= __class__.C_UINT64_MAX

        return mxid64

    # --------------------------------------------------------------------
    def GetXidConverter(
        sourceControlData: PgControlData, targetControlData: PgControlData
    ) -> any:
        assert type(sourceControlData) == PgControlData
        assert type(targetControlData) == PgControlData
        assert type(sourceControlData.XidSize) == int
        assert type(targetControlData.XidSize) == int

        if sourceControlData.XidSize == 4 and targetControlData.XidSize == 4:
            return __class__.CorrectXid32ToXid32

        if sourceControlData.XidSize == 4 and targetControlData.XidSize == 8:
            return __class__.CorrectXid32ToXid64

        if sourceControlData.XidSize == 8 and targetControlData.XidSize == 8:
            return __class__.CorrectXid64ToXid64

        raise RuntimeError(
            "Can't detect a MXid converter. Source XID size is {0}. Target XID size is {1}.".format(
                sourceControlData.XidSize, targetControlData.XidSize
            )
        )

    # --------------------------------------------------------------------
    def CorrectXid32ToXid32(xid32: PgXid) -> PgXid:
        assert type(xid32) == PgXid
        assert type(xid32.epoch) == int
        assert type(xid32.number) == int
        assert xid32.number >= 3
        assert xid32.number <= __class__.C_UINT32_MAX
        assert xid32.epoch >= 0

        return xid32.copy()

    # --------------------------------------------------------------------
    def CorrectXid32ToXid64(xid32: PgXid) -> PgXid:
        assert type(xid32) == PgXid
        assert type(xid32.epoch) == int
        assert type(xid32.number) == int
        assert xid32.number >= 3
        assert xid32.number <= __class__.C_UINT32_MAX
        assert xid32.epoch >= 0
        assert xid32.epoch <= __class__.C_UINT32_MAX

        r = xid32.epoch * __class__.C_4GB + xid32.number

        assert r >= 0
        assert r <= __class__.C_UINT64_MAX

        return PgXid.create(r)

    # --------------------------------------------------------------------
    def CorrectXid64ToXid64(xid64: PgXid) -> PgXid:
        assert type(xid64) == PgXid
        assert type(xid64.epoch) == int
        assert type(xid64.number) == int
        assert xid64.number >= 3
        assert xid64.number <= __class__.C_UINT64_MAX
        assert xid64.epoch == 0

        return xid64.copy()

    # Helper methods -----------------------------------------------------
    def Helper__DetectXidSize(dataDir: str) -> int:
        assert type(dataDir) == str
        assert dataDir != ""

        multixact_offsets_dir = os.path.join(dataDir, "pg_multixact/offsets")

        if not os.path.exists(multixact_offsets_dir):
            raise RuntimeError(
                "Can't detect xid size. Path [{0}] is not found.".format(
                    multixact_offsets_dir
                )
            )

        if not os.path.isdir(multixact_offsets_dir):
            raise RuntimeError(
                "Can't detect xid size. Path [{0}] is not directory.".format(
                    multixact_offsets_dir
                )
            )

        if __class__.Helper__IsFile(multixact_offsets_dir, "0000"):
            return 4

        if __class__.Helper__IsFile(multixact_offsets_dir, "000000000000000"):
            return 8

        raise RuntimeError(
            "Can't detect xid size. Path of a cluster is [{0}].".format(dataDir)
        )

    # --------------------------------------------------------------------
    def Helper__IsFile(path: str, fileName: str) -> bool:
        assert type(path) == str
        assert type(fileName) == str

        x = os.path.join(path, fileName)

        if os.path.exists(x):
            return True

        return False


# /////////////////////////////////////////////////////////////////////////////
