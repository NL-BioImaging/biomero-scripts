# BIOMERO scripts test harness

The deployable `master` branch intentionally contains no Python test files:
users may clone it directly into OMERO's recursively scanned `lib/scripts`
directory. The test suite is maintained on the `test-suite` branch instead.

Tests run locally against this branch's source by default. CI sets
`BIOMERO_SCRIPTS_ROOT` to a separate checkout of the exact pull-request or
`master` commit being tested:

```powershell
$env:BIOMERO_SCRIPTS_ROOT = "D:\path\to\biomero-scripts"
python -m pytest tests -q
```

Keep tests under `tests/` on this branch. Do not merge this branch into
`master`; update it independently whenever production behavior changes.
