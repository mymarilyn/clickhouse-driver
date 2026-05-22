"""
ClickHouse ``JSON`` (a.k.a. ``Object('json')``) column reader.

The wire format is the one introduced in ClickHouse 24.8+ and stabilised
in 25.x. The deserialization is composed out of the existing column
readers rather than hand-rolled:

  * ``DynamicColumn`` reads one dynamic path's body (a SerializationVariant
    over the declared variant types plus an implicit ``SharedVariant``).
  * ``ArrayColumn`` + ``TupleColumn`` + ``String`` reads the shared-data
    sub-column ``Array(Tuple(String, String))``.
  * Shared-variant blobs are decoded by walking ``encodeDataType`` and
    then delegating the value bytes to the appropriate column reader
    obtained via ``column_by_spec_getter``.

The write side still uses the ad-hoc format the original PR introduced.
Replacing it with a symmetric V2 writer is a separate piece of work; for
the SELECT path the asymmetry is harmless because the server already
accepts the legacy V1/V2 mix on insert.
"""

from .arraycolumn import ArrayColumn
from .base import Column
from .dynamiccolumn import (
    DYNAMIC_V2,
    SHARED_VARIANT_NAME,
    DynamicColumn,
    VARIANT_MODE_BASIC,
    decode_shared_value,
)
from .stringcolumn import ByteString, String
from .tuplecolumn import TupleColumn
from ..reader import (
    read_binary_str,
    read_binary_uint64,
)
from ..util.compat import json
from ..varint import read_varint, write_varint
from ..writer import write_binary_uint64


# ObjectSerializationVersion values (ClickHouse 25.5).
OBJECT_V1 = 0
OBJECT_STRING = 1
OBJECT_V2 = 2


class NewJsonColumn(Column):
    py_types = (dict, )

    # No NULL value actually
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self._column_kwargs = kwargs
        self.string_column = String(**kwargs)

        self.serialization_version = None
        self.sorted_dynamic_paths = []
        self.dynamic_columns = []
        self.shared_data_column = None

        super(NewJsonColumn, self).__init__(**kwargs)

    # ------------------------------------------------------------------
    # read path
    # ------------------------------------------------------------------

    def read_state_prefix(self, buf):
        # ObjectStructure stream layout (V1/V2):
        #   UInt64 LE  serialization_version
        #   if V1:     VarUInt max_dynamic_paths      (discarded)
        #   VarUInt    dynamic_paths_count
        #   N × String dynamic path names (sorted)
        #   (optional statistics — disabled in native SELECT by default)
        # Then, per dynamic path: SerializationDynamic state prefix.
        # Then, shared_data state prefix (no bytes — Array/Tuple/String
        # do not emit prefixes).
        version = read_binary_uint64(buf)
        if version == OBJECT_STRING:
            self.serialization_version = OBJECT_STRING
            return
        if version not in (OBJECT_V1, OBJECT_V2):
            raise NotImplementedError(
                "Unsupported JSON serialization version {}".format(version))
        self.serialization_version = version

        if version == OBJECT_V1:
            read_varint(buf)  # legacy max_dynamic_paths

        num_paths = read_varint(buf)
        self.sorted_dynamic_paths = [
            read_binary_str(buf) for _ in range(num_paths)
        ]

        self.dynamic_columns = []
        for _ in range(num_paths):
            col = DynamicColumn(
                self.column_by_spec_getter, **self._column_kwargs)
            col.read_state_prefix(buf)
            self.dynamic_columns.append(col)

        self.shared_data_column = self._build_shared_data_column()
        self.shared_data_column.read_state_prefix(buf)

    def read_items(self, n_items, buf):
        if self.serialization_version == OBJECT_STRING:
            return self._read_items_string(n_items, buf)

        # Typed paths declared in the JSON column spec (``JSON(name
        # Int64)``) would be read here in spec order. The current PR
        # does not parse typed paths out of the column-type string, so
        # we emulate the original behaviour and skip straight to dynamic
        # paths.
        column_per_path_values = []
        for col in self.dynamic_columns:
            column_per_path_values.append(col.read_items(n_items, buf))

        shared_rows = self.shared_data_column.read_data(n_items, buf)

        return self._fold_rows(
            n_items, column_per_path_values, shared_rows)

    def _read_items_string(self, n_items, buf):
        # Each row is one String containing the JSON text.
        strings = buf.read_strings(n_items, encoding='utf-8')
        return [json.loads(s) if s else {} for s in strings]

    def _build_shared_data_column(self):
        kwargs = self._column_kwargs
        key_column = String(**kwargs)
        # SharedVariant values are raw bytes (``encodeDataType +
        # serializeBinary``), not UTF-8 text.
        value_column = ByteString(**kwargs)
        tuple_column = TupleColumn(
            ('path', 'value'), [key_column, value_column], **kwargs)
        return ArrayColumn(tuple_column, **kwargs)

    def _fold_rows(self, n_items, per_path_values, shared_rows):
        rows = [{} for _ in range(n_items)]

        for path, values in zip(self.sorted_dynamic_paths, per_path_values):
            for row_idx, value in enumerate(values):
                if value is not None:
                    rows[row_idx][path] = _tuples_to_lists(value)

        for row_idx, entries in enumerate(shared_rows or []):
            for path, encoded in entries:
                value = decode_shared_value(
                    encoded, self.column_by_spec_getter)
                rows[row_idx][path] = _tuples_to_lists(value)

        for row in rows:
            self._denormalize_dotted_paths(row)

        return rows

    @staticmethod
    def _denormalize_dotted_paths(obj):
        for key in list(obj.keys()):
            parts = key.split('.')
            if len(parts) <= 1:
                continue
            parent = obj
            for part in parts[:-1]:
                if part not in parent or not isinstance(
                        parent[part], dict):
                    parent[part] = {}
                parent = parent[part]
            parent[parts[-1]] = obj[key]
            del obj[key]

    # ------------------------------------------------------------------
    # write path
    # ------------------------------------------------------------------

    def write_state_prefix(self, buf):
        # UInt64 LE: ObjectSerializationVersion::V2 (the server falls
        # back to V1 itself if its client revision is too old, so we can
        # write V2 unconditionally).
        write_binary_uint64(OBJECT_V2, buf)

    def write_items(self, items, buf, depth=0):
        # Convert all items to dictionaries.
        items = [
            x if not isinstance(x, str) else json.loads(x) for x in items]

        paths = self._unfold_json(items, depth)

        self._write_paths(paths, buf)
        self._write_specs(paths, buf)
        self._write_values(paths, len(items), buf)

    def _write_paths(self, paths, buf):
        # VarUInt dynamic_paths_count + N × String path names.
        write_varint(len(paths), buf)
        self.string_column.write_items(paths.keys(), buf)

    def _write_specs(self, paths, buf, depth=0):
        # SerializationDynamic state prefix per dynamic path:
        #   UInt64 LE  DynamicSerializationVersion::V2
        #   VarUInt    num_dynamic_types (excludes SharedVariant)
        #   N × String variant type spec
        # SerializationVariant prefix:
        #   UInt64 LE  discriminators_mode (BASIC)
        for col in paths.values():
            write_binary_uint64(DYNAMIC_V2, buf)
            write_varint(len(col), buf)
            self.string_column.write_items(col.keys(), buf)
            write_binary_uint64(VARIANT_MODE_BASIC, buf)
            for spec in col:
                if spec.startswith("Tuple") and "JSON" in spec:
                    self._write_complex_tuple_header(
                        col, spec, depth + 1, buf)
                elif spec.startswith("Array") and "JSON" in spec:
                    self._write_complex_array_header(
                        col, spec, depth + 1, buf)

    def _write_values(self, paths, rows, buf, depth=0):
        # SerializationVariant body per dynamic path:
        #   n_items × UInt8 global discriminators
        #   per declared variant in alphabetical order: column data
        # Followed by the shared-data Array(Tuple(String, String))'s
        # offsets — all zero because we never overflow into the
        # SharedVariant on write.
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
        for i, subspec in enumerate(spec[6:-2].split("), ")):
            if subspec.startswith("JSON"):
                self.write_state_prefix(buf)
                items = [item[i] for item in col[spec]["values"]]
                paths = self._unfold_json(items, depth=depth)
                self._write_paths(paths, buf)
                self._write_specs(paths, buf, depth=depth)

    def _write_complex_array_header(self, col, spec, depth, buf):
        self.write_state_prefix(buf)
        items = []
        for item in col[spec]["values"]:
            items += item
        paths = self._unfold_json(items, depth=depth)
        self._write_paths(paths, buf)
        self._write_specs(paths, buf, depth=depth)

    def _write_complex_tuple_values(self, col, spec, depth, buf):
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
        # Compute each row's global discriminator. ``DataTypeVariant``
        # sorts its variant list (which includes the implicit
        # ``SharedVariant``) by ``getName()`` alphabetically; the global
        # discriminator index is each variant's position in that sort.
        sorted_with_shared = sorted(
            list(col.keys()) + [SHARED_VARIANT_NAME])
        spec_to_discriminator = {
            spec: i for i, spec in enumerate(sorted_with_shared)
        }
        result = [255] * row_count
        for spec, data in col.items():
            disc = spec_to_discriminator[spec]
            for pos in data["positions"]:
                result[pos] = disc
        return bytes(result)

    def _normalize_json(self, obj,):
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
        result = {}
        for row, obj in enumerate(items):
            result = self._unfold_json_item(obj, depth, result, row)
        for k in list(result.keys()):
            result[k[:-1]] = dict(sorted(result[k].items()))
            del result[k]
        result = dict(sorted(result.items()))
        return result

    def _preprocess_array(self, values, array_type):
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


def _tuples_to_lists(value):
    """
    Normalise the Python types a JSON value comes back as so the surface
    matches the historical behaviour of ``NewJsonColumn``:

      * ``ArrayColumn`` already returns lists for ``Array(...)``.
      * ``TupleColumn`` returns tuples for ``Tuple(...)`` variants.
      * If any tuple contains a ``dict`` somewhere inside, ClickHouse
        stored the value as ``Tuple(..., JSON, ...)`` to carry an
        embedded JSON object — surface that as a Python ``list`` to
        match the original ``_read_complex_tuple_values`` output.
      * Tuples that do not contain a dict (e.g. fixed-shape ``Tuple(Int,
        String, Array)``) are left as tuples.
    """
    if isinstance(value, dict):
        return {k: _tuples_to_lists(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        items = [_tuples_to_lists(v) for v in value]
        if isinstance(value, list) or any(
                _contains_dict(item) for item in items):
            return items
        return tuple(items)
    return value


def _contains_dict(value):
    if isinstance(value, dict):
        return True
    if isinstance(value, (list, tuple)):
        return any(_contains_dict(v) for v in value)
    return False


def create_newjson_column(spec, column_by_spec_getter, column_options):
    return NewJsonColumn(column_by_spec_getter, **column_options)
