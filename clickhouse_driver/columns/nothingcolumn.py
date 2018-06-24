from .intcolumn import FormatColumn


class NothingColumn(FormatColumn):
    ch_type = 'Nothing'
    format = 'B'

    @property
    def size(self):
        return 1

    def after_read_item(self, buf):
        return None
