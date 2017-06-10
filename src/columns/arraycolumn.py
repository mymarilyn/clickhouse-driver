import six

from .base import Column
from .intcolumn import UInt64Column

if six.PY3:
    from queue import Queue
else:
    from Queue import Queue


class ArrayColumn(Column):
    """
    Nested arrays written in flatten form after information about their
    sizes (offsets really).
    One element of array of arrays can be represented as tree:
    (0 depth)          [[3, 4], [5, 6]]
                      /               \
    (1 depth)      [3, 4]           [5, 6]
                   /    \           /    \
    (leaf)        3      4         5      6

    Offsets (sizes) written in breadth-first search order. In example above
    following sequence of offset will be written: 4 -> 2 -> 4
    1) size of whole array: 4
    2) size of array 1 in depth=1: 2
    3) size of array 2 plus size of all array before in depth=1: 2 + 2 = 4

    After sizes info comes flatten data: 3 -> 4 -> 5 -> 6
    """
    py_types = (list, tuple)

    def __init__(self, nested_column):
        self.size_column = UInt64Column()
        self.nested_column = nested_column
        self._write_depth_0_size = True
        super(ArrayColumn, self).__init__()

    def write_data(self, data, buf):
        # Column of Array(T) is stored in "compact" format and passed to server
        # wrapped into another Array without size of wrapper array.
        self.nested_column = ArrayColumn(self.nested_column)
        self._write_depth_0_size = False
        super(ArrayColumn, self).write_data((data, ), buf)

    def read_data(self, rows, buf):
        self.nested_column = ArrayColumn(self.nested_column)
        return self._read_data(rows, buf)

    def _write_sizes(self, value, buf):
        q = Queue()
        q.put((self, value, 0))

        cur_depth = 0
        offset = 0
        while not q.empty():
            column, value, depth = q.get_nowait()

            if cur_depth != depth:
                cur_depth = depth
                offset = 0

            offset += len(value)
            if (cur_depth == 0 and self._write_depth_0_size) or cur_depth > 0:
                self.size_column.write(offset, buf)

            nested_column = column.nested_column
            if isinstance(nested_column, ArrayColumn):
                for x in value:
                    q.put((nested_column, x, cur_depth + 1))

    def _write_data(self, value, buf):
        if isinstance(self.nested_column, ArrayColumn):
            for x in value:
                self.nested_column._write_data(x, buf)
        else:
            for x in value:
                self.nested_column.write(x, buf)

    def write(self, value, buf):
        self._write_sizes(value, buf)
        self._write_data(value, buf)

    def _read_data(self, size, buf):
        q = Queue()
        q.put((self, size, 0))

        data = []
        slices_series = []

        cur_depth = 0
        prev_offset = 0
        slices = []
        # Read and store info about slices.
        while not q.empty():
            column, size, depth = q.get_nowait()

            if cur_depth != depth:
                cur_depth = depth
                prev_offset = 0
                slices_series.append(slices)
                slices = []

            nested_column = column.nested_column
            if isinstance(nested_column, ArrayColumn):
                for _i in range(size):
                    offset = self.size_column.read(buf)
                    q.put((nested_column, offset - prev_offset, cur_depth + 1))
                    slices.append((prev_offset, offset))
                    prev_offset = offset

            # Read data
            else:
                data.extend(nested_column.read(buf) for _i in range(size))

        # Build nested tuple structure.
        for slices in reversed(slices_series):
            nested_data = []
            for slice_from, slice_to in slices:
                nested_data.append(tuple(data[slice_from:slice_to]))
            data = nested_data

        return tuple(data)


def create_array_column(spec, column_by_spec_getter):
    inner = spec[6:-1]
    return ArrayColumn(column_by_spec_getter(inner))
