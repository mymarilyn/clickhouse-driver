import pandas as pd

from ..lowcardinalitycolumn import LowCardinalityColumn
from ...reader import read_binary_uint64
from .intcolumn import (
    NumpyUInt8Column, NumpyUInt16Column, NumpyUInt32Column, NumpyUInt64Column
)


class NumpyLowCardinalityColumn(LowCardinalityColumn):
    int_types = {
        0: NumpyUInt8Column,
        1: NumpyUInt16Column,
        2: NumpyUInt32Column,
        3: NumpyUInt64Column
    }

    def __init__(self, nested_column, **kwargs):
        super(NumpyLowCardinalityColumn, self).__init__(nested_column,
                                                        **kwargs)

    def _read_data(self, n_items, buf, nulls_map=None):
        if not n_items:
            return tuple()

        serialization_type = read_binary_uint64(buf)

        # Lowest byte contains info about key type.
        key_type = serialization_type & 0xf
        keys_column = self.int_types[key_type]()

        nullable = self.nested_column.nullable
        # Prevent null map reading. Reset nested column nullable flag.
        self.nested_column.nullable = False

        index_size = read_binary_uint64(buf)
        index = self.nested_column.read_data(index_size, buf)

        read_binary_uint64(buf)  # number of keys
        keys = keys_column.read_data(n_items, buf)

        if nullable:
            # Shift all codes by one ("No value" code is -1 for pandas
            # categorical) and drop corresponding first index
            # this is analog of original operation:
            # index = (None, ) + index[1:]
            keys = keys - 1
            index = index[1:]
        result = pd.Categorical.from_codes(keys, index)
        return result


def create_numpy_low_cardinality_column(spec, column_by_spec_getter):
    inner = spec[15:-1]
    nested = column_by_spec_getter(inner)
    return NumpyLowCardinalityColumn(nested)
