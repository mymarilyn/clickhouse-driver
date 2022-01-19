
from .arraycolumn import create_array_column


def create_nested_column(spec, column_by_spec_getter):
    return create_array_column(
        'Array(Tuple({}))'.format(','.join(get_nested_columns(spec))),
        column_by_spec_getter=column_by_spec_getter
    )


def get_nested_columns(spec):
    brackets = 0
    column_begin = 0

    inner_spec = get_inner_spec(spec)
    nested_columns = []
    for i, x in enumerate(inner_spec + ','):
        if x == ',':
            if brackets == 0:
                nested_columns.append(inner_spec[column_begin:i])
                column_begin = i + 1
        elif x == '(':
            brackets += 1
        elif x == ')':
            brackets -= 1
        elif x == ' ':
            if brackets == 0:
                column_begin = i + 1
    return nested_columns


def get_columns_with_types(spec):
    brackets = 0
    prev_comma = 0
    prev_space = 0

    inner_spec = get_inner_spec(spec)
    columns_with_types = []

    for i, x in enumerate(inner_spec + ','):
        if x == ',':
            if brackets == 0:
                columns_with_types.append((
                    inner_spec[prev_comma:prev_space].strip(),
                    inner_spec[prev_space:i]
                ))
                prev_comma = i + 1
        elif x == '(':
            brackets += 1
        elif x == ')':
            brackets -= 1
        elif x == ' ':
            if brackets == 0:
                prev_space = i + 1
    return columns_with_types


def get_inner_spec(spec):
    brackets = 0
    offset = len('Nested')
    i = offset
    for i, ch in enumerate(spec[offset:], offset):
        if ch == '(':
            brackets += 1

        elif ch == ')':
            brackets -= 1

        if brackets == 0:
            break

    return spec[offset + 1:i]
