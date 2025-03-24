# //////////////////////////////////////////////////////////////////////////////
# Postgres Professional (Russia).

from __future__ import annotations

# //////////////////////////////////////////////////////////////////////////////
# class PgCTID


class PgCTID:
    page: int
    offset: int

    # --------------------------------------------------------------------
    def __init__(self, page: int, offset: int):
        assert type(page) == int
        assert type(offset) == int

        self.page = page
        self.offset = offset

    # --------------------------------------------------------------------
    def create_from_string(ctid: str) -> PgCTID:
        assert ctid is not None
        assert type(ctid) == str

        ctid_len = len(ctid)

        i = 0

        if i == ctid_len:
            __class__.helper__throw_err__bad_ctid_format("#001")

        assert ctid_len > 0

        if ctid[i] != "(":
            __class__.helper__throw_err__bad_ctid_format("#002")

        i += 1

        i1s = i

        while i != ctid_len and ctid[i].isdigit():
            i += 1

        if i == i1s:
            __class__.helper__throw_err__bad_ctid_format("#003")

        i1e = i

        if i == ctid_len:
            __class__.helper__throw_err__bad_ctid_format("#004")

        if ctid[i] != ",":
            __class__.helper__throw_err__bad_ctid_format("#005")

        i += 1

        i2s = i

        while i != ctid_len and ctid[i].isdigit():
            i += 1

        if i == i2s:
            __class__.helper__throw_err__bad_ctid_format("#006")

        i2e = i

        if i == ctid_len:
            __class__.helper__throw_err__bad_ctid_format("#007")

        if ctid[i] != ")":
            __class__.helper__throw_err__bad_ctid_format("#008")

        i += 1

        if i != ctid_len:
            __class__.helper__throw_err__bad_ctid_format("#009")

        # OK!
        ctid_page = int(ctid[i1s:i1e])
        ctid_offset = int(ctid[i2s:i2e])

        # Let's check a result of our dreadful work
        assert "({0},{1})".format(ctid_page, ctid_offset) == ctid

        return __class__(ctid_page, ctid_offset)

    # Helper methods -----------------------------------------------------
    def helper__throw_err__bad_ctid_format(checkpoint: str):
        raise Exception("Bad format of CTID (checkpoint {0}).".format(checkpoint))


# //////////////////////////////////////////////////////////////////////////////
