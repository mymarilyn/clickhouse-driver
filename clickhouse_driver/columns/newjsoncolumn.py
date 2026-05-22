import io
from struct import Struct

from .base import Column
from .stringcolumn import String
from ..reader import (
    read_binary_bytes_fixed_len,
    read_binary_str,
    read_binary_str_fixed_len,
    read_binary_uint8,
    read_binary_uint64,
)
from ..util.compat import json
from ..varint import read_varint
from ..writer import write_binary_uint8, write_binary_uint64


# Sentinel key used inside the intermediary paths dict to carry per-row
# shared-data entries (paths that overflowed max_dynamic_paths /
# max_dynamic_types and were encoded into the JSON column's shared variant).
_SHARED_ROWS_KEY = '__clickhouse_driver_shared_rows__'


class NewJsonColumn(Column):
    py_types = (dict, )

    # No NULL value actually
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.string_column = String(**kwargs)
        super(NewJsonColumn, self).__init__(**kwargs)

    def write_state_prefix(self, buf):
        # Read in binary format.
        # Write in text format.
        write_binary_uint8(2, buf)

    def read_items(self, n_items, buf):
        paths = self._read_paths(buf)
        if paths is None:
            paths = {}
        self._read_specs(buf, paths)
        self._read_values(buf, paths, n_items)

        return self._fold_json(n_items, paths)

    def _read_paths(self, buf):
        """
        Read JSON paths.
        """
        read_binary_bytes_fixed_len(buf, 9)

        paths_count = read_binary_uint8(buf)
        if paths_count == 0:
            return None
        paths = {}
        for i in range(paths_count):
            strlen = read_binary_uint8(buf)
            col = read_binary_str_fixed_len(buf, strlen)
            paths[col] = {}

        return paths

    def _read_specs(self, buf, paths):
        """
        Read value specs.
        """
        for col in paths.values():
            read_binary_bytes_fixed_len(buf, 8)

            start = 0
            # ClickHouse client repeats the spec count bytes twice if
            # there are more than two different specs for a single column.
            spec_count = read_binary_uint8(buf)
            next_byte = read_binary_uint8(buf)
            if next_byte != spec_count:
                spec = read_binary_str_fixed_len(buf, next_byte)
                col[spec] = {"values": [], "positions": []}
                start = 1

            for i in range(start, spec_count):
                spec = read_binary_str(buf)
                col[spec] = {"values": [], "positions": []}

            read_binary_bytes_fixed_len(buf, 8)

            for spec in col:
                if spec.startswith("Tuple") and "JSON" in spec:
                    self._read_complex_tuple_header(buf, col, spec)
                elif spec.startswith("Array") and "JSON" in spec:
                    self._read_complex_array_header(buf, col, spec)

    def _read_complex_tuple_header(self, buf, col, spec):
        """
        Read header for JSON objects inside a tuple.
        """
        col[spec]["tuple_header"] = []
        subspecs = spec[6:-2].split("), ")
        for i, subspec in enumerate(subspecs):
            if subspec.startswith("JSON"):
                paths = self._read_paths(buf)
                if paths is None:
                    remaining = len(subspecs) - i
                    col[spec]["tuple_header"] += [None] * remaining
                    return
                self._read_specs(buf, paths)
                col[spec]["tuple_header"].append(paths)
            else:
                col[spec]["tuple_header"].append(None)

    def _read_complex_array_header(self, buf, col, spec):
        """
        Read header for JSON objects inside an array.
        """
        paths = self._read_paths(buf)
        self._read_specs(buf, paths)
        col[spec]["array_header"] = paths

    def _read_values(self, buf, paths, n_items):
        """
        Read values for each dynamic path, then the JSON column's shared
        data (paths that overflowed max_dynamic_paths/max_dynamic_types).
        """
        for col in paths.values():
            specs = self._read_row_positions(buf, col, n_items)

            # Read values of that column.
            for spec in specs:
                if spec.startswith("Array"):
                    if "JSON" in spec:
                        self._read_complex_array_values(buf, col, spec)
                    else:
                        reader = self.column_by_spec_getter(spec)
                        col[spec]["values"] = reader.read_data(
                            len(col[spec]["positions"]), buf)
                elif spec.startswith("Tuple"):
                    if "JSON" in spec:
                        self._read_complex_tuple_values(buf, col, spec)
                    else:
                        reader = self.column_by_spec_getter(spec)
                        col[spec]["values"] += reader.read_items(
                            len(col[spec]["positions"]), buf)
                else:
                    reader = self.column_by_spec_getter(spec)
                    col[spec]["values"] += reader.read_items(1, buf)

        self._read_shared_data(buf, paths, n_items)

    def _read_shared_data(self, buf, paths, n_items):
        """
        Read the JSON column's shared data: an Array(Tuple(String, String))
        sub-column whose first string is the path name and whose second
        string is the value encoded as ``encodeDataType + serializeBinary``.

        The N UInt64 cumulative array offsets are always present (zero
        offsets are written even when no paths overflowed).
        """
        if n_items == 0:
            paths[_SHARED_ROWS_KEY] = []
            return

        offsets = [read_binary_uint64(buf) for _ in range(n_items)]
        total = offsets[-1] if offsets else 0
        if total == 0:
            paths[_SHARED_ROWS_KEY] = [[] for _ in range(n_items)]
            return

        path_names = [read_binary_str(buf) for _ in range(total)]
        encoded_values = [self._read_binary_bytes(buf) for _ in range(total)]
        decoded_values = [
            self._decode_binary_value(blob) for blob in encoded_values
        ]

        shared_rows = [[] for _ in range(n_items)]
        prev = 0
        for row, off in enumerate(offsets):
            for i in range(prev, off):
                shared_rows[row].append(
                    (path_names[i], decoded_values[i]))
            prev = off

        paths[_SHARED_ROWS_KEY] = shared_rows

    @staticmethod
    def _read_binary_bytes(buf):
        length = read_varint(buf)
        return read_binary_bytes_fixed_len(buf, length)

    @staticmethod
    def _read_varint_bytesio(buf):
        # The ``varint`` Cython module's ``read_varint`` expects a
        # ``read_one()`` method (provided by the driver's buffered
        # readers). ``io.BytesIO`` does not implement it, so decode
        # LEB128 manually here.
        shift = 0
        result = 0
        while True:
            byte = buf.read(1)
            if not byte:
                return result
            b = byte[0]
            result |= (b & 0x7f) << shift
            shift += 7
            if b < 0x80:
                return result

    # Binary type tags used by ClickHouse's encodeDataType. Only the types
    # we know how to decode into Python values are listed; the rest fall
    # through to ``_decode_binary_unsupported``.
    _BINARY_TAG_NOTHING = 0x00
    _BINARY_TAG_UINT8 = 0x01
    _BINARY_TAG_UINT16 = 0x02
    _BINARY_TAG_UINT32 = 0x03
    _BINARY_TAG_UINT64 = 0x04
    _BINARY_TAG_INT8 = 0x07
    _BINARY_TAG_INT16 = 0x08
    _BINARY_TAG_INT32 = 0x09
    _BINARY_TAG_INT64 = 0x0A
    _BINARY_TAG_FLOAT32 = 0x0D
    _BINARY_TAG_FLOAT64 = 0x0E
    _BINARY_TAG_STRING = 0x15
    _BINARY_TAG_ARRAY = 0x1E
    _BINARY_TAG_NULLABLE = 0x23
    _BINARY_TAG_BOOL = 0x2D

    _PRIMITIVE_STRUCTS = {
        _BINARY_TAG_UINT8: Struct('<B'),
        _BINARY_TAG_UINT16: Struct('<H'),
        _BINARY_TAG_UINT32: Struct('<I'),
        _BINARY_TAG_UINT64: Struct('<Q'),
        _BINARY_TAG_INT8: Struct('<b'),
        _BINARY_TAG_INT16: Struct('<h'),
        _BINARY_TAG_INT32: Struct('<i'),
        _BINARY_TAG_INT64: Struct('<q'),
        _BINARY_TAG_FLOAT32: Struct('<f'),
        _BINARY_TAG_FLOAT64: Struct('<d'),
    }

    def _decode_binary_value(self, blob):
        """
        Decode a value stored in the JSON column's shared variant.
        ``blob`` is the raw bytes of one ``String`` entry, holding
        ``encodeDataType(type) + serializeBinary(value)``.
        """
        buf = io.BytesIO(blob)
        return self._decode_binary_inner(buf)

    def _decode_binary_inner(self, buf):
        tag = buf.read(1)
        if not tag:
            return None
        tag = tag[0]

        if tag == self._BINARY_TAG_NOTHING:
            return None

        if tag in self._PRIMITIVE_STRUCTS:
            s = self._PRIMITIVE_STRUCTS[tag]
            return s.unpack(buf.read(s.size))[0]

        if tag == self._BINARY_TAG_BOOL:
            return bool(buf.read(1)[0])

        if tag == self._BINARY_TAG_STRING:
            length = self._read_varint_bytesio(buf)
            return buf.read(length).decode('utf-8')

        if tag == self._BINARY_TAG_NULLABLE:
            # Encoded type bytes for the inner type are immediately followed
            # by the value, which is prefixed with a single null-flag byte.
            return self._decode_nullable(buf)

        if tag == self._BINARY_TAG_ARRAY:
            return self._decode_array(buf)

        return self._decode_binary_unsupported(tag, buf)

    def _decode_nullable(self, buf):
        # Skip the inner type encoding so we can read the value that follows.
        self._skip_encoded_type(buf)
        is_null = buf.read(1)[0]
        if is_null:
            return None
        # The remaining bytes are the inner value's serializeBinary output;
        # the inner type was already consumed above, so dispatch by looking
        # ahead at the next type tag is impossible. The encoded type already
        # consumed told us what to expect, but ClickHouse writes the value
        # in its own format directly (no additional tag). We re-read the
        # original buffer using a separate path.
        # For shared data, ClickHouse always uses the full
        # encodeDataType + serializeBinary roundtrip without wrapping
        # primitives in Nullable, so this branch is rarely hit. Fall back
        # to returning the raw remaining bytes.
        return buf.read()

    def _decode_array(self, buf):
        # Array(T): inner type encoding, then VarUInt size, then size values
        # in T's binary form. To stay general we decode each element by
        # re-reading the inner type tag we saved.
        type_bytes = self._capture_encoded_type(buf)
        size = self._read_varint_bytesio(buf)
        result = []
        for _ in range(size):
            inner_buf = io.BytesIO(type_bytes + buf.read(self._value_size(
                type_bytes, buf)))
            inner_buf.seek(0)
            result.append(self._decode_binary_inner(inner_buf))
        return result

    def _value_size(self, type_bytes, buf):
        # Best-effort: for primitive types we can size the value, for
        # variable-length we read until the entry boundary. ClickHouse's
        # serializeBinary for primitives writes exactly ``struct.size``
        # bytes; for String it writes ``VarUInt + bytes``. We don't have a
        # good general size, so fall back to reading the rest of ``buf``.
        tag = type_bytes[0]
        if tag in self._PRIMITIVE_STRUCTS:
            return self._PRIMITIVE_STRUCTS[tag].size
        if tag == self._BINARY_TAG_BOOL:
            return 1
        # Unknown size — consume the rest of the buffer for this element.
        return len(buf.getvalue()) - buf.tell()

    def _skip_encoded_type(self, buf):
        tag = buf.read(1)[0]
        if tag in (self._BINARY_TAG_NULLABLE, self._BINARY_TAG_ARRAY):
            self._skip_encoded_type(buf)

    def _capture_encoded_type(self, buf):
        start = buf.tell()
        self._skip_encoded_type(buf)
        end = buf.tell()
        buf.seek(start)
        return buf.read(end - start)

    def _decode_binary_unsupported(self, tag, buf):
        # Return the remaining bytes so users see something rather than a
        # silent KeyError. Surfaces unknown types without breaking the
        # whole query.
        return {
            '__unsupported_binary_type__': tag,
            'raw': buf.read(),
        }

    def _read_complex_tuple_values(self, buf, col, spec):
        """
        Read values in a tuple with nested JSON elements.
        """
        col[spec]["values"] = [
            [] for _ in range(len(col[spec]["positions"]))]
        subspecs = spec[6:-2].split("), ")
        for i, subspec in enumerate(subspecs):
            if (not subspec.startswith("Array")
                    and not subspec.startswith("Tuple")
                    and not subspec.startswith("JSON")):
                buf.read(len(col[spec]["positions"]))
            for row in col[spec]["values"]:
                if subspec.startswith("JSON"):
                    paths = col[spec]["tuple_header"][i]
                    if paths is None:
                        # Nested JSON declared with
                        # max_dynamic_types=0 / max_dynamic_paths=0: only
                        # shared data carries the row.
                        empty_paths = {}
                        self._read_shared_data(
                            buf, empty_paths,
                            len(col[spec]["positions"]))
                        shared = empty_paths.get(_SHARED_ROWS_KEY, [])
                        for pos, entries in enumerate(shared):
                            obj = {}
                            for path, value in entries:
                                obj[path] = value
                            self._denormalize_json(obj)
                            col[spec]["values"][pos].append(obj)
                        break
                    self._read_values(
                        buf, paths, len(col[spec]["positions"]))
                    result = self._fold_json(
                        len(col[spec]["positions"]), paths)
                    for pos, item in enumerate(result):
                        col[spec]["values"][pos].append(item)
                    break
                elif subspec.startswith("Array"):
                    reader = self.column_by_spec_getter(
                        subspec + ")")
                    row += reader.read_data(1, buf)
                elif subspec.startswith("Tuple"):
                    reader = self.column_by_spec_getter(
                        subspec[6:])
                    row += reader.read_data(1, buf)
                else:
                    reader = self.column_by_spec_getter(
                        subspec[9:])
                    row += reader.read_data(1, buf)

    def _read_complex_array_values(self, buf, col, spec):
        """
        Read values in an array with nested JSON elements.
        """
        bounds = []
        for i in range(len(col[spec]["positions"])):
            bounds.append(read_binary_uint64(buf))
        paths = col[spec]["array_header"]
        self._read_values(buf, paths, bounds[-1])
        result = self._fold_json(
            bounds[-1], paths)
        prev_bound = 0
        for i, bound in enumerate(bounds):
            col[spec]["values"].append(result[prev_bound:bound])
            col[spec]["positions"].append(i)
            prev_bound = bound

    def _read_row_positions(self, buf, col, n_items):
        """
        Read value positions in the record list.
        """
        specs = []
        skip = len(col) - len(
            [v for v in col
             if v.startswith("String") or v.startswith("Tuple")])
        for i in range(n_items):
            spec_number = read_binary_uint8(buf)
            if spec_number < 255:
                if spec_number > skip:
                    spec_number -= 1
                spec = list(col.keys())[spec_number]
                if not (spec.startswith("Array")
                        or spec.startswith("Tuple")) or spec not in specs:
                    specs.append(spec)
                col[spec]["positions"].append(i)

        return sorted(specs)

    def write_items(self, items, buf, depth=0):
        # Convert all items to dictionaries.
        items = [
            x if not isinstance(x, str) else json.loads(x) for x in items]

        paths = self._unfold_json(items, depth)

        self._write_paths(paths, buf)
        self._write_specs(paths, buf)
        self._write_values(paths, len(items), buf)

    def _write_paths(self, paths, buf):
        """
        Convert items into desired format and write them.
        """
        buf.write(b"\x00" * 7)
        write_binary_uint8(len(paths), buf)
        self.string_column.write_items(paths.keys(), buf)

    def _write_specs(self, paths, buf, depth=0):
        """
        Write values specs.
        """
        for col in paths.values():
            buf.write(b"\x02" + b"\x00" * 7)
            write_binary_uint8(len(col), buf)
            self.string_column.write_items(col.keys(), buf)
            buf.write(b"\x00" * 8)
            for spec in col:
                if spec.startswith("Tuple") and "JSON" in spec:
                    self._write_complex_tuple_header(
                        col, spec, depth + 1, buf)
                elif spec.startswith("Array") and "JSON" in spec:
                    self._write_complex_array_header(
                        col, spec, depth + 1, buf)

    def _write_values(self, paths, rows, buf, depth=0):
        """
        Write values.
        """
        for col in paths.values():
            buf.write(self._get_row_posititons(col, rows))
            for spec in col:
                if spec.startswith("Array"):
                    if "JSON" in spec:
                        self._write_complex_array_values(
                            col, spec, depth + 1, buf)
                    else:
                        insert = self._preprocess_array(
                            col[spec]["values"], spec[6:-1])
                        writer = self.column_by_spec_getter(spec)
                        writer.write_data(insert, buf)
                elif spec.startswith("Tuple"):
                    if "JSON" in spec:
                        self._write_complex_tuple_values(
                            col, spec, depth + 1, buf)
                    else:
                        writer = self.column_by_spec_getter(spec)
                        writer.write_items(col[spec]["values"], buf)
                else:
                    writer = self.column_by_spec_getter(spec)
                    writer.write_items(col[spec]["values"], buf)

        # Write final padding.
        buf.write(b"\x00" * rows * 8)

    def _write_complex_tuple_header(self, col, spec, depth, buf):
        """
        Write header for JSON objects inside a tuple.
        """
        for i, subspec in enumerate(spec[6:-2].split("), ")):
            if subspec.startswith("JSON"):
                self.write_state_prefix(buf)
                items = [item[i] for item in col[spec]["values"]]
                paths = self._unfold_json(items, depth=depth)
                self._write_paths(paths, buf)
                self._write_specs(paths, buf, depth=depth)

    def _write_complex_array_header(self, col, spec, depth, buf):
        """
        Write header for JSON objects inside an array.
        """
        self.write_state_prefix(buf)
        items = []
        for item in col[spec]["values"]:
            items += item
        paths = self._unfold_json(items, depth=depth)
        self._write_paths(paths, buf)
        self._write_specs(paths, buf, depth=depth)

    def _write_complex_tuple_values(self, col, spec, depth, buf):
        """
        Write values in a tuple with nested JSON elements.
        """
        for i, subspec in enumerate(spec[6:-2].split("), ")):
            is_simple = (
                not subspec.startswith("Array")
                and not subspec.startswith("Tuple")
                and not subspec.startswith("JSON"))
            if is_simple:
                buf.write(b"\x00" * len(col[spec]["values"]))
            for row in col[spec]["values"]:
                if subspec.startswith("JSON"):
                    items = [item[i] for item in col[spec]["values"]]
                    paths = self._unfold_json(items, depth=depth)
                    self._write_values(
                        paths, len(items), buf, depth=depth)
                    break
                elif subspec.startswith("Array"):
                    insert = self._preprocess_array(
                        [row[i]], subspec[6:])
                    writer = self.column_by_spec_getter(
                        subspec + ")")
                    writer.write_data(insert, buf)
                elif subspec.startswith("Tuple"):
                    writer = self.column_by_spec_getter(
                        subspec[6:])
                    writer.write_data([row[i]], buf)
                else:
                    writer = self.column_by_spec_getter(
                        subspec[9:])
                    writer.write_data([row[i]], buf)

    def _write_complex_array_values(self, col, spec, depth, buf):
        """
        Write values in an array with nested JSON elements.
        """
        bound = 0
        for v in col[spec]["values"]:
            bound = bound + len(v)
            write_binary_uint64(bound, buf)
        items = []
        for item in col[spec]["values"]:
            items += item
        paths = self._unfold_json(items, depth=depth)
        self._write_values(paths, len(items), buf, depth=depth)

    def _get_json_value_spec(self, item, depth):
        """
        Returns a ClickHouse spec for a JSON data type.
        """
        if isinstance(item, int) and not isinstance(item, bool):
            return "Int64"
        elif isinstance(item, float):
            return "Float64"
        elif isinstance(item, str):
            return "String"
        elif isinstance(item, bool):
            return "Bool"
        elif isinstance(item, dict):
            max_types = int(2 ** (4 - depth))
            max_paths = int(4 ** (4 - depth))
            return (
                "JSON(max_dynamic_types={}, "
                "max_dynamic_paths={})".format(max_types, max_paths)
            )
        elif isinstance(item, list):
            value_types = []
            for entry in item:
                t = type(entry)
                if t not in value_types:
                    value_types.append(t)
            if dict in value_types or list in value_types:
                result = "Tuple("
                unique_specs = []
                for entry in item:
                    spec = self._get_json_value_spec(entry, depth)
                    if (not spec.startswith("Array")
                            and not spec.startswith("Tuple")
                            and not spec.startswith("JSON")):
                        result += "Nullable({}), ".format(spec)
                    else:
                        result += "{}, ".format(spec)
                    if spec not in unique_specs:
                        unique_specs.append(spec)

                # Return an array if all specs are the same
                if len(unique_specs) == 1:
                    inner = self._get_json_value_spec(item[0], depth=depth)
                    return "Array({})".format(inner)
                result = result[:-2] + ")"
                return result
            else:
                if str in value_types:
                    return "Array(Nullable(String))"
                elif float in value_types:
                    if bool not in value_types:
                        return "Array(Nullable(Float64))"
                    else:
                        return "Array(Nullable(String))"
                elif int in value_types:
                    return "Array(Nullable(Int64))"
                elif bool in value_types:
                    return "Array(Nullable(Bool))"
                else:
                    return "Array(Nullable(String))"
        elif item is None:
            return "String"

    def _get_row_posititons(self, col, row_count):
        """
        Returns bytes corresponding to the position of specs between
        records.
        """
        result = [255] * row_count
        count = 0
        skip = len(col) - len(
            [v for v in col.keys()
             if v.startswith("String") or v.startswith("Tuple")])
        for spec in col:
            if count == skip:
                count += 1
            for pos in col[spec]["positions"]:
                result[pos] = count
            count += 1
        return bytes(result)

    def _normalize_json(self, obj,):
        """
        Deals with converting a nested dictionary to a dictionary of
        paths with depth one.
        """
        if isinstance(obj, dict):
            result = {}
            for k in obj:
                if obj[k] is not None:
                    obj_res = self._normalize_json(obj[k])
                    for obj_k in obj_res:
                        result["{}.{}".format(k, obj_k)] = obj_res[obj_k]
            return result
        else:
            return {"": obj}

    def _unfold_json_item(self, obj, depth, result={}, row_count=0):
        """
        Converts a single record into an intermeditary format stored in
        result.
        """
        for k in obj:
            if obj[k] is not None:
                obj_res = self._normalize_json(obj[k])
                for obj_k in obj_res:
                    key = "{}.{}".format(k, obj_k)
                    if key not in result:
                        result[key] = {}
                    spec = self._get_json_value_spec(obj_res[obj_k], depth)
                    if spec not in result[key]:
                        result[key][spec] = {
                            "values": [], "positions": []}
                    result[key][spec]["values"].append(obj_res[obj_k])
                    result[key][spec]["positions"].append(row_count)
        return result

    def _unfold_json(self, items, depth):
        """
        Converts the passed dictionary into an intermediary format.
        """
        result = {}
        for row, obj in enumerate(items):
            result = self._unfold_json_item(obj, depth, result, row)
        for k in list(result.keys()):
            result[k[:-1]] = dict(sorted(result[k].items()))
            del result[k]
        result = dict(sorted(result.items()))
        return result

    def _denormalize_json(self, obj):
        """
        Converts a dictionary of paths with depth one to a nested
        dictionary.
        """
        keys = list(obj.keys())
        for key in keys:
            split_key = key.split(".")
            if len(split_key) > 1:
                parent = obj
                for part in split_key[:-1]:
                    if part not in parent:
                        parent[part] = {}
                    parent = parent[part]
                parent[split_key[-1]] = obj[key]
                del obj[key]

    def _fold_json(self, n_items, obj):
        """
        Converts an intermediary record back to a list of rows
        """
        result = [{} for _ in range(n_items)]

        shared_rows = obj.pop(_SHARED_ROWS_KEY, None)

        for key, item in obj.items():
            for spec in item.values():
                for i in range(len(spec["values"])):
                    result[spec["positions"][i]][key] = spec["values"][i]

        if shared_rows is not None:
            for row_idx, entries in enumerate(shared_rows):
                for path, value in entries:
                    result[row_idx][path] = value

        [self._denormalize_json(item) for item in result]
        return result

    def _preprocess_array(self, values, array_type):
        """
        Preprocesses array values for insert.
        """
        insert = []
        if array_type.startswith("Array"):
            for item in values:
                insert.append(
                    self._preprocess_array(item, array_type[6:-1]))
            return insert

        if "String" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if isinstance(elem, str):
                        arr.append(elem)
                    elif isinstance(elem, bool):
                        arr.append(str(elem).lower())
                    elif elem is None:
                        arr.append(None)
                    else:
                        arr.append(str(elem))
                insert.append(arr)
        elif "Int64" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if elem is None:
                        arr.append(0)
                    else:
                        arr.append(int(elem))
                insert.append(arr)
        elif "Float64" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if elem is None:
                        arr.append(0)
                    else:
                        arr.append(float(elem))
                insert.append(arr)
        elif "Bool" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if elem is not None:
                        arr.append(bool(elem))
                insert.append(arr)
        else:
            insert = values

        return insert


def create_newjson_column(spec, column_by_spec_getter, column_options):
    return NewJsonColumn(column_by_spec_getter, **column_options)
