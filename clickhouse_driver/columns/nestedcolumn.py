
from .arraycolumn import create_array_column
from .util import get_inner_spec, get_inner_columns, \
    get_inner_columns_with_types


def create_nested_column(spec, column_by_spec_getter):
    return create_array_column(
        'Array(Tuple({}))'.format(','.join(get_nested_columns(spec))),
        column_by_spec_getter=column_by_spec_getter
    )


def get_nested_columns(spec):
    inner_spec = get_inner_spec('Nested', spec)
    return get_inner_columns(inner_spec)


def get_columns_with_types(spec):
    inner_spec = get_inner_spec('Nested', spec)
    return get_inner_columns_with_types(inner_spec)
