<!--
Link to relevant issues or previous PRs, one per line. Use "fixes" to
automatically close an issue.
-->

- fixes #<issue number>

<!--
Ensure each step in CONTRIBUTING.rst is complete by adding an "x" to
each box below.

If only docs were changed, these aren't relevant and can be removed.
-->

Checklist:

- [ ] Add tests that demonstrate the correct behavior of the change. Tests should fail without the change.
- [ ] Add or update relevant docs, in the docs folder and in code.
- [ ] Ensure PR doesn't contain untouched code reformatting: spaces, etc.
- [ ] Run `flake8` and fix issues.
- [ ] Run `pytest` no tests failed. See https://clickhouse-driver.readthedocs.io/en/latest/development.html.
