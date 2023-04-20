
DEFAULT_DATABASE = ''
DEFAULT_USER = 'default'
DEFAULT_PASSWORD = ''

DEFAULT_PORT = 9000
DEFAULT_SECURE_PORT = 9440

DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES = 50264
DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS = 51554
DBMS_MIN_REVISION_WITH_BLOCK_INFO = 51903
# Legacy above.
DBMS_MIN_REVISION_WITH_CLIENT_INFO = 54032
DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058
DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372
DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401
DBMS_MIN_REVISION_WITH_SERVER_LOGS = 54406
DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA = 54410
DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420
DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429
DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET = 54441
DBMS_MIN_REVISION_WITH_OPENTELEMETRY = 54442
DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH = 54448
DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME = 54449
DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS = 54451
DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS = 54453
DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION = 54454

# Timeouts
DBMS_DEFAULT_CONNECT_TIMEOUT_SEC = 10
DBMS_DEFAULT_TIMEOUT_SEC = 300

DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC = 5

DEFAULT_COMPRESS_BLOCK_SIZE = 1048576
DEFAULT_INSERT_BLOCK_SIZE = 1048576

DBMS_NAME = 'ClickHouse'
CLIENT_NAME = 'python-driver'
CLIENT_VERSION_MAJOR = 20
CLIENT_VERSION_MINOR = 10
CLIENT_VERSION_PATCH = 2
CLIENT_REVISION = DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION

BUFFER_SIZE = 1048576

STRINGS_ENCODING = 'utf-8'
