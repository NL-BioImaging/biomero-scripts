# biomero-scripts

OMERO Python scripts that export image data, run containerized workflows on a
remote Slurm cluster through the `biomero` library, and import results with
recorded provenance. Deployed on the OMERO server as part of NL-BIOMERO; script
`VERSION` declarations are locked to the required `biomero` release.

## Read first

1. Shared rules for all component repositories:
   <https://github.com/NL-BioImaging/NL-BIOMERO/blob/master/.agents/skills/nl-biomero-engineering/SKILL.md>
2. Independent PR review procedure:
   <https://github.com/NL-BioImaging/NL-BIOMERO/blob/master/.agents/skills/nl-biomero-review/SKILL.md>
3. Script hierarchy, release versioning, test-harness and documentation rules:
   [`.agents/skills/biomero-scripts-dev/SKILL.md`](.agents/skills/biomero-scripts-dev/SKILL.md)

## Layout

- `__workflows/`: workflow entry points (`SLURM_Run_Workflow.py`, batched runner, example).
- `_data/`: transfer, conversion, update-polling and result scripts.
- `admin/`: initialization, setup check, migration and diagnostic scripts.
- `.github/workflows/tests.yml`: CI; runs the harness from the `test-suite` branch.
- Never add Python tests or a `tests/` directory to `master` or a production
  branch: OMERO may expose them as scripts. Tests live on `test-suite`.

## Build and check

- Compile and whitespace check (from the repo root):
  `python -m compileall -q admin _data __workflows` and `git diff --check`.
- Tests (checked out from `test-suite` into a separate worktree):
  `BIOMERO_SCRIPTS_ROOT=<this checkout>`, `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`,
  then `python -m pytest tests -q`. CI uses Python 3.11.
- Find script versions: `rg -n '^VERSION\s*=' . -g '*.py'`.

## Related repositories

biomero (core), NL-BIOMERO, BIOMERO.importer, OMERO.biomero,
BIOMERO.shallower, biomero-schema, BIOMERO.ZarrViewer, all under
<https://github.com/NL-BioImaging>. Each has its own `AGENTS.md`.
