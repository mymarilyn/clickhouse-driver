# Changelog

## Unreleased

## [0.2.3] - 2022-02-07
### Added
- `tzlocal`>=4.0 support. Pull request [#263](https://github.com/mymarilyn/clickhouse-driver/pull/263) by [azat](https://github.com/azat).
- `quota_key` support.
- Wheels for Python 3.10.
- Bool type. Pull request [#279](https://github.com/mymarilyn/clickhouse-driver/pull/279) by [adrian17](https://github.com/adrian17).
- Nested type with `flatten_nested=0`. Pull request [#285](https://github.com/mymarilyn/clickhouse-driver/pull/285) by [spff](https://github.com/spff).

### Fixed
- Handle partially consumed query. Solves issue [#117](https://github.com/mymarilyn/clickhouse-driver/issues/117).
- Fallback to generic columns when NumPy support is not implemented for column type. Solves issue [#254](https://github.com/mymarilyn/clickhouse-driver/issues/254).
- Broken ZSTD decompression. Solves issue [#269](https://github.com/mymarilyn/clickhouse-driver/issues/269).
- External tables passing with NumPy. Solves issue [#267](https://github.com/mymarilyn/clickhouse-driver/issues/267).
- Consider tzinfo for datetime parameters substitution. Solves issue [#268](https://github.com/mymarilyn/clickhouse-driver/issues/268).
- Do not use NumPy columns inside generic columns. Solves issue [#272](https://github.com/mymarilyn/clickhouse-driver/issues/272).
- Decimal128 and Decimal256 types_check. Solves issue [#274](https://github.com/mymarilyn/clickhouse-driver/issues/274).
- Insertion using `execute` in DB API. Solves issue [#179](https://github.com/mymarilyn/clickhouse-driver/issues/179). Pull request [#276](https://github.com/mymarilyn/clickhouse-driver/pull/276) by [nnseva](https://github.com/nnseva).
- Variables cannot be declared with `cpdef` in Cython 3. Pull request [#281](https://github.com/mymarilyn/clickhouse-driver/pull/281) by [ym](https://github.com/ym).

### Changed
- Switch from nose test runner to pytest.
- Migrate from Travis CI to GitHub Actions.

## [0.2.2] - 2021-09-24
### Added
- DateTime64 extended range. Pull request [#222](https://github.com/mymarilyn/clickhouse-driver/pull/222) by [0x4ec7](https://github.com/0x4ec7).
- Support for using `Client` as context manager closing connection on exit. Solves issue [#237](https://github.com/mymarilyn/clickhouse-driver/issues/237). Pull request [#206](https://github.com/mymarilyn/clickhouse-driver/pull/238) by [wlhjason](https://github.com/wlhjason).
- OpenTelemetry support. Solves issue [#230](https://github.com/mymarilyn/clickhouse-driver/issues/230).
- `tzlocal`>=3.0 support.
- Date32 type.
- NumPy Nullable(T) support.

### Fixed
- Boxing on DataFrames INSERT.
- Empty external tables sending. Solves issue [#240](https://github.com/mymarilyn/clickhouse-driver/issues/240).
- Create error message before disconnect. Pull request [#247](https://github.com/mymarilyn/clickhouse-driver/pull/247) by [NikiforovG](https://github.com/NikiforovG).
- Writing explicit NumPy `NaN` values. Solves issue [#249](https://github.com/mymarilyn/clickhouse-driver/issues/249).
- UInt128 INSERTing. Solves issue [#251](https://github.com/mymarilyn/clickhouse-driver/issues/251).
- Large DataFrames chunking. Solves issue [#243](https://github.com/mymarilyn/clickhouse-driver/issues/243).
- Allow arbitrary DataFrame columns order on INSERT. Solves issue [#245](https://github.com/mymarilyn/clickhouse-driver/issues/245).

### Changed
- Protocol version bumped to 54442.
- Unpin `lz4` for CPython. Pin `lz4`<=3.0.1 only for PyPy.

### Removed
- `transpose` parameter from `insert_dataframe`.

## [0.2.1] - 2021-06-02
### Added
- Linux wheels for AArch64. Pull request [#197](https://github.com/mymarilyn/clickhouse-driver/pull/197) by [odidev](https://github.com/odidev).
- [U]Int28/256 types.
- Decimal256 type.
- Code coverage for cython code.
- Map type.
- Support for private key/certificate file. Pull request [#219](https://github.com/mymarilyn/clickhouse-driver/pull/219) by [alexole](https://github.com/alexole).

### Fixed
- Empty Array(Tuple(T)) writing. Solves issue [#177](https://github.com/mymarilyn/clickhouse-driver/issues/177).
- Preserve Decimal precision on INSERT. Solves issue [#192](https://github.com/mymarilyn/clickhouse-driver/issues/192).
- Remove closed cursors from connection. Solves issue [#194](https://github.com/mymarilyn/clickhouse-driver/issues/194).
- DB API connect with default params.
- Fix log blocks handling. Solves issue [#203](https://github.com/mymarilyn/clickhouse-driver/issues/203).
- Multidimensional Array decoding. Solves issue [#204](https://github.com/mymarilyn/clickhouse-driver/issues/204). Pull request [#206](https://github.com/mymarilyn/clickhouse-driver/pull/206) by [smagellan](https://github.com/smagellan).
- Use last database after reconnect. Solves issue [#205](https://github.com/mymarilyn/clickhouse-driver/issues/205).
- Decimal(N, 1) handling. Pull request [#210](https://github.com/mymarilyn/clickhouse-driver/pull/210) by [raw0w](https://github.com/raw0w).

### Changed
- Decimal128 performance increased (up to 25% compared to 0.2.0 release).

### Removed
- Python 2.7 support.

## [0.2.0] - 2020-12-14
### Added
- NumPy reading/writing for columns: Float32/64, [U]Int8/16/32/64, Date/DateTime(‘timezone’)/DateTime64(‘timezone’), String/FixedString(N), LowCardinality(T). Merge [Arturus's](https://github.com/Arturus/clickhouse-driver) fork.
- pandas DataFrame reading/writing.
- Ability to mark all settings as important to fail on unknown settings on sever side.
- SSL SNI support. Solves issue [#172](https://github.com/mymarilyn/clickhouse-driver/issues/172).
- Wheels for Python 3.9 and PyPy.
- Remember last successful host on connection. Solves issue [#168](https://github.com/mymarilyn/clickhouse-driver/issues/168).

### Fixed
- Server logs displaying on INSERT.
- Make exceptions picklable. Pull request [#169](https://github.com/mymarilyn/clickhouse-driver/pull/169) by [azat](https://github.com/azat).
- Enum type deserializing when it wrapped in SimpleAggregateFunction. Pull request [#170](https://github.com/mymarilyn/clickhouse-driver/pull/170) by [flyAwayGG](https://github.com/flyAwayGG).
- Pin major `tzlocal` version. Solves issue [#166](https://github.com/mymarilyn/clickhouse-driver/issues/166).

### Changed
- String and DateTime columns writing optimization.
- Array columns reading/writing optimization.
- Chunking optimization for large lists/tuples.
- Protocol version bumped to 54441.

## [0.1.5] - 2020-09-19
### Added
- Do not require settings declaration if server support setting-as-string. Pull request [#142](https://github.com/mymarilyn/clickhouse-driver/pull/142) by [azat](https://github.com/azat).
- `host_name` in logs. Pull request [#144](https://github.com/mymarilyn/clickhouse-driver/pull/144) by [azat](https://github.com/azat).
- Cursor attribute `columns_with_types` to DB API. Issue [#149](https://github.com/mymarilyn/clickhouse-driver/issues/149).
- Cursor method `set_query_id` to DB API. Issue [#152](https://github.com/mymarilyn/clickhouse-driver/issues/152).

### Fixed
- Connection error messages formatting.
- `Client.from_url` credentials unquoting. Issue [#146](https://github.com/mymarilyn/clickhouse-driver/issues/146).
- Empty nested array handling. Pull request [#161](https://github.com/mymarilyn/clickhouse-driver/pull/161) by [dourvaris](https://github.com/dourvaris).
- `read_varint` overflow. Issue [#163](https://github.com/mymarilyn/clickhouse-driver/issues/163).
- Malformed reads/writes in `BufferedReader`. This addresses [CVE-2020-26759](https://nvd.nist.gov/vuln/detail/CVE-2020-26759).

### Changed
- Use deque for ~4x speedup when reading Array columns. Pull request [#164](https://github.com/mymarilyn/clickhouse-driver/pull/164) by [dourvaris](https://github.com/dourvaris).

## [0.1.4] - 2020-06-13
### Added
- Tuple type.
- Custom String column encoding.
- Settings update to v20.4.1.2742 server version. Pull request [#133](https://github.com/mymarilyn/clickhouse-driver/pull/133) by [azat](https://github.com/azat).
- Settings update to v20.5.1.3657 server version. Pull request [#141](https://github.com/mymarilyn/clickhouse-driver/pull/141) by [azat](https://github.com/azat).
- Unsupported server versions to documentation.
- Performance section to documentation.
- Python 3.9 in Travis CI build matrix.

### Fixed
- Reading/writing Array(Tuple).
- 20.x server version support.
- Settings mutation in `execute`.
- Slow columnar results returning (`columnar=True`).
- Segfault on passing not encoded strings during `INSERT` into ByteString column.

### Changed
- Miscellaneous read/write optimizations
- Protocol version bumped to 54429.

## [0.1.3] - 2020-02-21
### Added
- Python DB API 2.0.
- Multiple hosts support on connection errors.
- Insert columnar data support. Pull request [#122](https://github.com/mymarilyn/clickhouse-driver/pull/122) by [Anexen](https://github.com/Anexen).
- Wheels for Python 3.8.
- DateTime64 type.
- Settings update to v20.2.1.2201 server version. Pull request [#123](https://github.com/mymarilyn/clickhouse-driver/pull/123) by [azat](https://github.com/azat).

### Fixed
- `Client.from_url` settings detection.
- Close socket on `KeyboardInterrupt` while running query.
- Null handling in LowCardinality columns.

### Changed
- Protocol version bumped to 54421.
- Increased speed (up to 20-30% compared to 0.1.2 release) on heavy `SELECT` and `INSERT` queries. Pull request [#122](https://github.com/mymarilyn/clickhouse-driver/pull/122) by [Anexen](https://github.com/Anexen).
- Memory consumption decreased (up to 20% compared to 0.1.2 release). Pull request [#122](https://github.com/mymarilyn/clickhouse-driver/pull/122) by [Anexen](https://github.com/Anexen).

## [0.1.2] - 2019-10-18
### Added
- Settings update to 19.16.1 server version. Pull request [#111](https://github.com/mymarilyn/clickhouse-driver/pull/111) by [azat](https://github.com/azat).
- Python 3.8 in Travis CI build matrix.
- Returning inserted rows count on `INSERT` queries with data. Returning rows count from `INSERT FROM SELECT` is not supported.

### Fixed
- Exposing `columnar` parameter to `execute_with_progress`. Pull request [#108](https://github.com/mymarilyn/clickhouse-driver/pull/108) by [igorbb](https://github.com/igorbb).
- LowCardinality tests. Pull request [#112](https://github.com/mymarilyn/clickhouse-driver/pull/112) by [azat](https://github.com/azat).

### Changed
- Increased speed (up to 5 times compared to 0.1.1 release) of `INSERT` queries.
- Date/DateTime columns selecting and inserting optimizations.

## [0.1.1] - 2019-09-20
### Added
- `Client.from_url` method that creates client configured from the given URL.

### Fixed
- If source column was timezone-aware values from DateTime column are returned with timezone now.
- Handling zero bytes in the middle of FixedString column. Issue [#104](https://github.com/mymarilyn/clickhouse-driver/issues/104).

## [0.1.0] - 2019-08-07
### Added
- SimpleAggregateFunction type. Pull request [#95](https://github.com/mymarilyn/clickhouse-driver/pull/95) by [azat](https://github.com/azat).

### Changed
- Increased speed (5-6 times compared to 0.0.20 release) of `SELECT` queries with large amount of strings.
- Package is distributed in source and binary forms now. Compilation from source is required for platforms without wheels.

### Fixed
- Elapsed time calculation on INSERT.
- Dependencies environment markers for poetry in `setup.py`. Pull request [#96](https://github.com/mymarilyn/clickhouse-driver/pull/96) by [nitoqq](https://github.com/nitoqq).

## [0.0.20] - 2019-06-02
### Added
- LowCardinality(T) type.
- Access for processed rows, bytes and elapsed time of the last executed query.
- Allow to insert `datetime` into Date column. Pull request [#75](https://github.com/mymarilyn/clickhouse-driver/pull/75) by [gle4er](https://github.com/gle4er).
- 'max_partitions_per_insert_block' setting. Pull request [#85](https://github.com/mymarilyn/clickhouse-driver/pull/85) by [mhsekhavat](https://github.com/mhsekhavat).

### Fixed
- Fallback for user name if it's not defined. Pull request [#87](https://github.com/mymarilyn/clickhouse-driver/pull/87) by [wawaka](https://github.com/wawaka).

## [0.0.19] - 2019-03-31
### Added
- IPv4/IPv6 types. Pull request [#73](https://github.com/mymarilyn/clickhouse-driver/pull/73) by [AchilleAsh](https://github.com/AchilleAsh).

### Fixed
- String enums escaping.

## [0.0.18] - 2019-02-19
### Fixed
- Strings mishandling read from buffer. Pull request [#72](https://github.com/mymarilyn/clickhouse-driver/pull/72) by [mitsuhiko](https://github.com/mitsuhiko).

## [0.0.17] - 2019-01-09
### Added
- Server logs displaying.
- Documentation on Read the Docs: https://clickhouse-driver.readthedocs.io

### Changed
- Protocol version bumped to 54406.

### Fixed
- INSERT generators support as data parameter.
- INSERT null value on ByteString column. Pull request [#65](https://github.com/mymarilyn/clickhouse-driver/pull/65) by [vivienm](https://github.com/vivienm).
- Integer types support in FloatColumn.
- Handle quotes and equation signs in Enum options. Pull request [#67](https://github.com/mymarilyn/clickhouse-driver/pull/67) by [sochi](https://github.com/sochi).

## [0.0.16] - 2018-10-09
### Added
- Option to access profile info about the last executed query. Pull request [#57](https://github.com/mymarilyn/clickhouse-driver/pull/57) by [vanzi](https://github.com/vanzi).
- Decimal type.
- Settings update to 18.14.2 server version.

### Changed
- String column read/write optimizations.
- Protocol version bumped to 54401.

### Fixed
- Client settings are not mutable now.

## [0.0.15] - 2018-09-26
### Fixed
- Unpin `clickhouse-cityhash` dependency.

## [0.0.14] - 2018-08-16
### Added
- Block by block results streaming.

## [0.0.13] - 2018-07-26
### Fixed
- Revert pull request [#45](https://github.com/mymarilyn/clickhouse-driver/pull/45) by [shotInLeg](https://github.com/shotInLeg) due to `INSERT FROM SELECT` breaking.

## [0.0.12] - 2018-07-25
### Added
- Allow for access of final progress stats. Pull request [#41](https://github.com/mymarilyn/clickhouse-driver/pull/41) by [alex-hofsteede](https://github.com/alex-hofsteede).
- Supplying raw integers in DateTime columns. Pull request [#42](https://github.com/mymarilyn/clickhouse-driver/pull/42) by [wawaka](https://github.com/wawaka).

### Fixed
- Pip install in editable mode.

### Changed
- Raise ValueError instead of timeout for INSERT queries without params. Pull request [#45](https://github.com/mymarilyn/clickhouse-driver/pull/45) by [shotInLeg](https://github.com/shotInLeg).

## [0.0.11] - 2018-06-03
### Added
- Timezone support in DateTime type.
- Python 3.7 and PyPy in Travis CI build matrix.
- Direct bytes support in FixedString. Pull request [#26](https://github.com/mymarilyn/clickhouse-driver/pull/26) by [lidalei](https://github.com/lidalei).

### Removed
- Python 3.3 support.

## [0.0.10] - 2018-03-14
### Added
- Server version specific tests.
- Nothing type.
- Travis CI build matrix.

### Fixed
- Possible IndexError in packet types string representation. Pull request [#28](https://github.com/mymarilyn/clickhouse-driver/pull/28) by [WouldYouKindly](https://github.com/WouldYouKindly).
- Do not use timezone in Date columns. Issue [#29](https://github.com/mymarilyn/clickhouse-driver/issues/29).
- Approximate rows to read calculation in `execute_with_progress`. Pull request [#30](https://github.com/mymarilyn/clickhouse-driver/pull/30) by [b1naryth1ef](https://github.com/b1naryth1ef).

### Changed
- List and Tuple types rendering in parameters substitution. Pull request [#27](https://github.com/mymarilyn/clickhouse-driver/pull/27) by [silentsokolov](https://github.com/silentsokolov).

## [0.0.9] - 2018-01-17
### Added
- Interval type. Pull request [#16](https://github.com/mymarilyn/clickhouse-driver/pull/16) by [kszucs](https://github.com/kszucs).

### Fixed
- Raise EOFError when no data is read in `_read_one` in Python 3.
- SSL file descriptors closing in Python 2.7.

### Changed
- Log level for normal operations raised to DEBUG. Pull request [#17](https://github.com/mymarilyn/clickhouse-driver/pull/17) by [kmatt](https://github.com/kmatt).

### Removed
- Drop Python 3.2 support due to lz4 issues.

## [0.0.8] - 2017-10-23
### Added
- Parameters substitution for SELECT queries.
- SSL support.

### Fixed
- Columnar result returning from multiple blocks. Columns must be concatenated.
- Settings logger was root.
- Reading/writing Array(String) NotImplementedError raise.
- IPv6 support issue [#12](https://github.com/mymarilyn/clickhouse-driver/issues/12).

## [0.0.7] - 2017-10-12
### Added
- Configurable logging level in tests.
- Full error codes list.
- Force check clickhouse-cityhash is installed if compression is used.
- `Client` can be directly imported from package.
- `insert_block_size` parameter - maximum rows in block (default is 1048576).
- Columnar result returning (`columnar=True`). Pull request [#11](https://github.com/mymarilyn/clickhouse-driver/pull/11) by [kszucs](https://github.com/kszucs).
- Tunable types check (`types_check=True`). Off by default.

### Changed
- Handling only socket-related errors on ping. Errors are logged with `WARNING` level.
- Client is created per one test.
- Sending/receiving data speed significantly increased.

### Fixed
- Version detection in setup.py.
- Error handling on socket.shutdown.
- Install `enum34` only if required.
- `clickhouse-cityhash` import issue [#10](https://github.com/mymarilyn/clickhouse-driver/issues/10).

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

[Unreleased]: https://github.com/mymarilyn/clickhouse-driver/compare/0.2.3...HEAD
[0.2.3]: https://github.com/mymarilyn/clickhouse-driver/compare/0.2.2...0.2.3
[0.2.2]: https://github.com/mymarilyn/clickhouse-driver/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/mymarilyn/clickhouse-driver/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/mymarilyn/clickhouse-driver/compare/0.1.5...0.2.0
[0.1.5]: https://github.com/mymarilyn/clickhouse-driver/compare/0.1.4...0.1.5
[0.1.4]: https://github.com/mymarilyn/clickhouse-driver/compare/0.1.3...0.1.4
[0.1.3]: https://github.com/mymarilyn/clickhouse-driver/compare/0.1.2...0.1.3
[0.1.2]: https://github.com/mymarilyn/clickhouse-driver/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/mymarilyn/clickhouse-driver/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.20...0.1.0
[0.0.20]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.19...0.0.20
[0.0.19]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.18...0.0.19
[0.0.18]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.17...0.0.18
[0.0.17]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.16...0.0.17
[0.0.16]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.15...0.0.16
[0.0.15]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.14...0.0.15
[0.0.14]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.13...0.0.14
[0.0.13]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.12...0.0.13
[0.0.12]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.11...0.0.12
[0.0.11]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.10...0.0.11
[0.0.10]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.9...0.0.10
[0.0.9]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.8...0.0.9
[0.0.8]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.7...0.0.8
[0.0.7]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.6...0.0.7
[0.0.6]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.5...0.0.6
[0.0.5]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/mymarilyn/clickhouse-driver/compare/0.0.2...0.0.3
