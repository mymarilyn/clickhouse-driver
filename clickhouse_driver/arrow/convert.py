import pyarrow as pa

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None

from ..protocol import ServerPacketTypes
from .mapping import get_type_and_converter


def create_record_batch_reader(packet_generator, context):
    """
    Creates RecordBatchReader yielding one record batch per ClickHouse
    block. Schema is built from the header block, so it's available before
    any data block is received.
    """
    strings_as_bytes = context.client_settings.get('strings_as_bytes', False)

    blocks = _data_blocks(packet_generator)

    first_block = next(blocks, None)
    if first_block is None:
        return pa.RecordBatchReader.from_batches(pa.schema([]), iter([]))

    columns_with_types = first_block.columns_with_types
    fields = [
        get_type_and_converter(type_, strings_as_bytes)
        for _, type_ in columns_with_types
    ]

    # Header block contains no rows.
    buffered = [first_block] if first_block.num_rows else []

    # Types without explicit mapping are inferred from the first non-empty
    # block. It has to be received before the schema can be built.
    if any(type_ is None for type_, _ in fields):
        while not buffered:
            block = next(blocks, None)
            if block is None:
                break
            if block.num_rows:
                buffered.append(block)

        fields = _infer_missing_types(fields, buffered)

    schema = pa.schema([
        pa.field(name, type_)
        for (name, _), (type_, _) in zip(columns_with_types, fields)
    ])

    def batches():
        for block in buffered:
            yield _block_to_batch(block, schema, fields)

        for block in blocks:
            if block.num_rows:
                yield _block_to_batch(block, schema, fields)

    return pa.RecordBatchReader.from_batches(schema, batches())


def _data_blocks(packet_generator):
    for packet in packet_generator:
        if getattr(packet, 'type', None) == ServerPacketTypes.DATA:
            block = getattr(packet, 'block', None)
            if block is not None:
                yield block


def _infer_missing_types(fields, buffered):
    rv = []
    for i, (type_, converter) in enumerate(fields):
        if type_ is None:
            if buffered:
                column = buffered[0].get_columns()[i]
                type_ = _column_to_array(column, None, converter).type
            else:
                type_ = pa.null()
        rv.append((type_, converter))
    return rv


def _block_to_batch(block, schema, fields):
    arrays = [
        _column_to_array(column, type_, converter)
        for column, (type_, converter) in zip(block.get_columns(), fields)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _column_to_array(column, type_, converter):
    # Numeric and datetime64 NumPy columns are handled by Arrow without
    # copying.
    if np is not None and isinstance(column, np.ndarray) and \
            column.dtype.kind in 'iufbM':
        return pa.array(column, type=type_)

    if pd is not None and isinstance(column, pd.Categorical):
        column = [None if pd.isna(x) else x for x in column]
    elif not isinstance(column, list):
        column = list(column)

    if converter is not None:
        column = [None if x is None else converter(x) for x in column]

    return pa.array(column, type=type_)
