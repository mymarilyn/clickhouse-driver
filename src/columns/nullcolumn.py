from .intcolumn import FormatColumn


class NullColumn(FormatColumn):
    ch_type = 'Null'
    format = 'B'

    @property
    def size(self):
        return 1

    def after_read_item(self, buf):
        return None
