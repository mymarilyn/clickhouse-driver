# Changelog

## [Unreleased]
### Added
- Configurable logging level in tests.
- Full error codes list.
- Force check clickhouse-cityhash is installed if compression is used.

### Changed
- Handling only socket-related errors on ping. Errors are logged with `WARNING` level.
- Client is created per one test.

### Fixed
- Version detection in setup.py.
- Error handling on socket.shutdown.

### Removed
- QuickLZ support
- six dependency

## [0.0.6] - 2017-09-19
### Added
- UUID type.
- Query limits settings.
- Code coverage.
- ClickHouse server and driver version upped to 54276.
- Changelog.
- Added column name to `TypeMismatchError`.
- Query progress information.
- Version of package.

### Fixed
- socket.timeout error handling on connect.

## [0.0.5] - 2017-07-16
### Added
- Nullable(T) type.
- Return data from TOTALS and EXTREMES packets.
- Query settings.
- query_id execution option.
- NULL type.
- Raise exception on SELECT queries.

### Changed
- Small columns refactoring.
- `clickhouse-client` in tests moved to docker.

## [0.0.4] - 2017-06-15
### Added
- FixedString(N) type.
- Enum8/16 types.
- Array(T) type.
- External data for query processing
- Raise UnknownTypeError for unsupported columns.

### Changed
- Socket connect timeout fix.

## [0.0.3] - 2017-05-24
### Added
- QuickLZ, LZ4/LZ4HC, ZSTD compressions.
- Support old servers without BlockInfo.
- Travis CI.
- flake8 syntax check.

## 0.0.2 - 2017-05-16
### Added
- [U]Int8/16/32/64 types.
- Date/DateTime types.
- String types.

[Unreleased]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.6...HEAD
[0.0.6]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.5...0.0.6
[0.0.5]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.2...0.0.3
