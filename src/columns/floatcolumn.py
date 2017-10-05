from ctypes import c_float

from .base import FormatColumn


class FloatColumn(FormatColumn):
    py_types = (float, int)


class Float32(FloatColumn):
    ch_type = 'Float32'
    format = 'f'

    def __init__(self, types_check=False, **kwargs):
        super(Float32, self).__init__(types_check=types_check, **kwargs)

        if types_check:
            # Chop only bytes that fit current type.
            # Cast to -nan or nan if overflows.
            def before_write_item(value):
                return c_float(value).value

            self.before_write_item = before_write_item


class Float64(FloatColumn):
    ch_type = 'Float64'
    format = 'd'
