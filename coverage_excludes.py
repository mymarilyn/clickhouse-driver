"""
Coverage.py plugin: exclude version-gated lines that the current
matrix entry cannot reach.

Lines tagged ``# pragma: requires-clickhouse-X.Y`` are excluded when
``$CLICKHOUSE_VERSION`` is older than ``X.Y``. Both ``coverage run``
and ``coveralls`` reload ``.coveragerc`` in separate processes; placing
the logic in a plugin lets the same exclusion list reach both. With
``$CLICKHOUSE_VERSION`` unset the plugin is a no-op, so local runs
measure every line.
"""
import os
import pathlib
import re

from coverage import CoveragePlugin


_PRAGMA = re.compile(r'pragma: requires-clickhouse-(\d+\.\d+)')


def _tuple(version):
    return tuple(int(part) for part in version.split('.'))


class VersionGatedExcludes(CoveragePlugin):
    def configure(self, config):
        target = os.getenv('CLICKHOUSE_VERSION', '')
        if not target:
            return
        target_t = _tuple(target)
        root = pathlib.Path(__file__).parent / 'clickhouse_driver'
        if not root.exists():
            return
        versions = set()
        for path in root.rglob('*.py'):
            versions.update(
                _PRAGMA.findall(path.read_text(encoding='utf-8')))
        excludes = list(config.get_option('report:exclude_lines') or [])
        for version in sorted(versions, key=_tuple):
            if _tuple(version) > target_t:
                pattern = 'pragma: requires-clickhouse-{}'.format(
                    re.escape(version))
                if pattern not in excludes:
                    excludes.append(pattern)
        config.set_option('report:exclude_lines', excludes)


def coverage_init(reg, options):
    reg.add_configurer(VersionGatedExcludes())
