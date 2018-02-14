import socket
import getpass

from . import defines
from . import errors
from .writer import write_binary_str, write_varint, write_binary_uint8


class ClientInfo(object):
    class Interface(object):
        TCP = 1
        HTTP = 2

    class QueryKind(object):
        # Uninitialized object.
        NO_QUERY = 0

        INITIAL_QUERY = 1

        # Query that was initiated by another query for distributed query
        # execution.
        SECONDARY_QUERY = 2

    client_version_major = defines.DBMS_VERSION_MAJOR
    client_version_minor = defines.DBMS_VERSION_MINOR
    client_revision = defines.CLIENT_VERSION
    interface = Interface.TCP

    initial_user = ''
    initial_query_id = ''
    initial_address = '0.0.0.0:0'

    quota_key = ''

    def __init__(self, client_name):
        self.query_kind = ClientInfo.QueryKind.NO_QUERY

        self.os_user = getpass.getuser()
        self.client_hostname = socket.gethostname()
        self.client_name = client_name

        super(ClientInfo, self).__init__()

    @property
    def empty(self):
        return self.query_kind == ClientInfo.QueryKind.NO_QUERY

    def write(self, server_revision, fout):
        revision = server_revision
        if server_revision < defines.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            raise errors.LogicalError('Method ClientInfo.write is called '
                                      'for unsupported server revision')

        write_binary_uint8(self.query_kind, fout)
        if self.empty:
            return

        write_binary_str(self.initial_user, fout)
        write_binary_str(self.initial_query_id, fout)
        write_binary_str(self.initial_address, fout)

        write_binary_uint8(self.interface, fout)

        write_binary_str(self.os_user, fout)
        write_binary_str(self.client_hostname, fout)
        write_binary_str(self.client_name, fout)
        write_varint(self.client_version_major, fout)
        write_varint(self.client_version_minor, fout)
        write_varint(self.client_revision, fout)

        if revision >= defines.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO:
            write_binary_str(self.quota_key, fout)
