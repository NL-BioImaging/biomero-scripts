---
name: biomero-scripts-dev
description: Develop, review, test, document, and release the OMERO Python scripts in the biomero-scripts repository. Use for changes to workflow orchestration, OMERO-to-HPC data transfer, Slurm execution and monitoring, result import, importer integration, administrative scripts, script VERSION declarations, or cross-repository compatibility with biomero and NL-BIOMERO.
---

# BIOMERO Scripts Development

Treat this repository as the OMERO-script layer of BIOMERO. The scripts let
OMERO users run containerized, FAIR bioimage-analysis workflows on a remote HPC
cluster. They rely on the `biomero` Python library for SSH/Slurm orchestration
and are deployed as part of the NL-BIOMERO stack.

## Understand the deployment boundary

- Install and register these scripts on the OMERO server.
- Route their execution to the specialized `biomeroworker` processor.
- Let the worker use `biomero` to connect to the remote HPC over SSH, submit
  Slurm jobs, monitor them, and retrieve their output.
- Keep containerized workflow versions, parameters, provenance, inputs, and
  outputs traceable. Preserve this FAIR and reproducible execution model.

Do not assume that the environment visible to OMERO.web is the environment of
the worker. Runtime environment variables that affect analysis normally need
to be configured on `biomeroworker`.

## Follow the script hierarchy

### Main workflow orchestration

`__workflows/SLURM_Run_Workflow.py` is the primary end-to-end entry point. It:

1. resolves the selected configured workflow and its parameters;
2. transfers selected OMERO image data to HPC storage;
3. transfers file-annotation inputs when required;
4. performs format conversion when required;
5. submits and monitors the workflow on Slurm;
6. imports or uploads results into OMERO;
7. records provenance and cleans temporary artifacts.

`__workflows/SLURM_Run_Workflow_Batched.py` sits above the main runner. It
splits input into batches, invokes multiple Run Workflow executions, and polls
the resulting jobs. Keep shared behavior in or aligned with the main runner;
do not let the batched wrapper silently diverge.

`__workflows/SLURM_CellPose_Segmentation.py` is an example single-workflow
script. It is not the normal entry point and is not installed by default in
NL-BIOMERO. It requires manual transfer and result retrieval.

### Data and job helper scripts

- `_data/_SLURM_Image_Transfer.py`: export selected OMERO images, datasets, or
  plates to HPC storage, normally as Zarr.
- `_data/_SLURM_File_Transfer.py`: copy one OMERO FileAnnotation, such as model
  weights or a CSV configuration, into the job input directory and return its
  remote path for the workflow argument.
- `_data/SLURM_Remote_Conversion.py`: perform remote format conversion,
  including the same-format no-op path.
- `_data/SLURM_Get_Update.py`: poll job state and parse percentage progress
  from workflow logs when the workflow emits it.
- `_data/SLURM_Get_Results.py`: classic result path; upload results through the
  OMERO API onto OMERO-managed storage.
- `_data/SLURM_Import_Results.py`: importer-enabled result path; stage results
  on shared remote storage and import them in place with BIOMERO.importer.

The worker selects the result script at runtime:

- `IMPORTER_ENABLED=false` (default): use `SLURM_Get_Results.py`.
- `IMPORTER_ENABLED=true`: use `SLURM_Import_Results.py`; require the importer
  dependency, shared storage mounts, group mappings, and worker permissions.

### Administrative scripts

- `admin/SLURM_Init_environment.py`: initialize and validate remote Slurm
  directories, generated job scripts, and workflow/converter images.
- `admin/SLURM_check_setup.py`: report and validate BIOMERO configuration,
  connectivity, workflow versions, and converter availability.
- `admin/Tail_logs.py`: let OMERO administrators inspect recent BIOMERO logs.
- `admin/Example_Minimal_Slurm_Script.py`: admin-only diagnostic/example script
  for restricted ad-hoc HPC commands. NL-BIOMERO removes it from production
  images by default; do not broaden access or install it by default.

## Keep releases locked to the BIOMERO library

Treat every script's `VERSION` as locked to the `biomero` library version that
the scripts require. Do not release the scripts with a different version from
their required library release.

Git does not enforce this coupling yet. Before every release:

1. determine the exact `biomero` release required by the script code;
2. update every Python `VERSION` declaration to that exact version;
3. search dynamically rather than assuming the current script count;
4. verify that only the intended release version remains;
5. confirm NL-BIOMERO installs a compatible library and scripts revision.

Use repository-root searches such as:

```powershell
rg -n '^VERSION\s*=' . -g '*.py'
rg -n -F 'OLD.VERSION' .
```

Do not update only the main runner. Admin, workflow, and data scripts expose
their versions to OMERO independently and must all be updated.

## Make changes safely

1. Inspect `git status` and preserve unrelated worktree changes.
2. Trace calls from the public workflow script into helper scripts and the
   corresponding `biomero` APIs before changing signatures or outputs.
3. Preserve OMERO rtype wrapping/unwrapping and script input/output contracts.
4. Preserve cleanup and error reporting; cleanup failures should not hide the
   primary workflow result or error.
5. Mock OMERO, SSH, Slurm, and importer boundaries in unit tests. Do not require
   a live OMERO server or HPC cluster for routine tests.
6. Compile all script directories after edits:

```powershell
python -m compileall -q admin _data __workflows
git diff --check
```

Run available targeted tests as well. If the checkout contains only cached test
artifacts and no test source, report that limitation rather than claiming tests
passed.

## Follow red/green TDD

Write behavior tests before changing production scripts and run them against the
unchanged production worktree. A new positive test must fail for the behavior it
is intended to add or fix; a test that has never been red is not evidence that
the change works.

- Start with the narrowest relevant test selection and capture the expected
  failure.
- Implement the smallest backward-compatible production change that makes the
  test pass.
- Rerun the narrow selection after each implementation batch, then the complete
  test harness before declaring the work done.
- Distinguish positive tests from guard tests that assert something does not
  happen. Guard tests may already pass before implementation, so pair them with
  at least one positive test that demonstrably goes red.
- If tests and implementation cannot be separated, temporarily neutralize the
  new production path, prove the positive regression test fails, then restore it
  and prove it passes.

Because tests live on `test-suite`, make the test commit in its separate
worktree and point `BIOMERO_SCRIPTS_ROOT` at the unchanged production worktree
for the red run. Do not use the source snapshot carried by `test-suite` as proof
of current production behavior.

## Keep tests off the deployable branch

Every production branch can be cloned directly into OMERO's recursively scanned
`lib/scripts` directory during development or deployment. Never add Python test
files or a `tests/` directory to `master` **or a production feature branch**,
because OMERO may expose them as runnable scripts.

The canonical test harness lives on the separate `test-suite` branch. The
workflow at `.github/workflows/tests.yml` checks out the triggering `master` or
pull-request revision as `source`, checks out `test-suite` as `harness`, and sets
`BIOMERO_SCRIPTS_ROOT` so the harness tests the exact source revision rather than
the source snapshot on its own branch.

When production behavior changes:

1. Make only the production edit on `master` or its feature branch. A pull
   request branch must remain test-file-free even when its tests are temporary,
   disabled in CI, or intended to be run only by hand.
2. Use a separate worktree checked out to `test-suite`; never merge
   `test-suite` into `master`.
3. Add or update tests under `tests/` in that worktree. Tests must resolve the
   source repository from `BIOMERO_SCRIPTS_ROOT`, with their own branch root only
   as a local fallback.
4. Run the harness against the production worktree, for example:

```powershell
$env:BIOMERO_SCRIPTS_ROOT = "D:\path\to\biomero-scripts"
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = "1"
python -m pytest tests -q
```

5. Commit and push production and test changes to their respective branches.
   If CI cannot select feature-specific tests yet, keep those tests on
   `test-suite` and run the relevant files manually with
   `BIOMERO_SCRIPTS_ROOT` pointed at the feature worktree. Do not solve the CI
   limitation by copying tests into the production branch. Keep
   `tests/requirements.txt` on `test-suite` aligned with test dependencies.

The workflow runs for pull requests and pushes to `master`. Coordinate test
branch updates with production pull requests so the required check exercises
the intended behavior.

## Keep cross-repository documentation aligned

Update this repository's `README.md` when script roles, inputs, security,
logging, installation, or result selection changes. Also inspect sibling repos:

- `biomero`: update library API/configuration documentation when a script
  depends on new or changed client behavior.
- `NL-BIOMERO`: update deployment documentation and Compose/container wiring
  when a change affects worker environment variables, mounts, permissions,
  processor routing, importer enablement, or installed script selection.

Remember that scripts use DEBUG rotating-file logging and INFO stdout logging
by default. Preserve useful operational diagnostics while avoiding secrets in
logs.
