import logging


def default_message_filter(record):
    record.msg = (
        f'[ {record.server_host_name} ] '
        f'[ {record.server_event_time}.'
        f'{record.server_event_time_microseconds:06d} ] '
        f'[ {record.server_thread_id} ] '
        f'{{{record.server_query_id}}} '
        f'<{record.server_priority}> '
        f'{record.server_source}: '
        f'{record.server_text}'
    )
    return True


def configure_logger(raw_log_record=False):
    logger = logging.getLogger(__name__)
    if raw_log_record:
        logger.removeFilter(default_message_filter)
    else:
        logger.addFilter(default_message_filter)
    return logger


logger = configure_logger()

# Keep in sync with ClickHouse priorities
# https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/InternalTextLogsQueue.cpp
log_priorities = (
    'Unknown',
    'Fatal',
    'Critical',
    'Error',
    'Warning',
    'Notice',
    'Information',
    'Debug',
    'Trace',
    'Test',
)

num_priorities = len(log_priorities)


def log_block(block):
    if block is None:
        return

    column_names = [x[0] for x in block.columns_with_types]

    for row in block.get_rows():
        row = dict(zip(column_names, row))

        if 1 <= row['priority'] <= num_priorities:
            row['priority'] = log_priorities[row['priority']]
        else:
            row['priority'] = row[0]

        # thread_number in servers prior 20.x
        row['thread_id'] = row.get('thread_id') or row['thread_number']

        # put log block row into LogRecord extra
        extra = {"server_"+k: v for k, v in row.items()}
        logger.info(row['text'], extra=extra)
