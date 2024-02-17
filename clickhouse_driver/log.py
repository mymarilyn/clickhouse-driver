import logging

logger = logging.getLogger(__name__)

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
            priority = log_priorities[row['priority']]
        else:
            priority = row[0]

        # thread_number in servers prior 20.x
        thread_id = row.get('thread_id') or row['thread_number']

        logger.info(
            '[ %s ] [ %s ] {%s} <%s> %s: %s',
            row['host_name'],
            thread_id,
            row['query_id'],
            priority,
            row['source'],
            row['text']
        )
