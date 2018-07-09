from . import defines
from .reader import read_varint


class Progress(object):
    def __init__(self):
        self.new_rows = 0
        self.new_bytes = 0
        self.new_total_rows = 0

        super(Progress, self).__init__()

    def read(self, server_revision, fin):
        self.new_rows = read_varint(fin)
        self.new_bytes = read_varint(fin)

        revision = server_revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS:
            self.new_total_rows = read_varint(fin)
