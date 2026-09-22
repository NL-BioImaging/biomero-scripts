# Provenance alignment plan

## Status and scope

The current provenance fix keeps the complete `metadata.csv` available and
prevents one oversized MapAnnotation from blocking the remaining annotations.
It does not yet make every result path interpret that file in the same way.

This plan describes the follow-up work required to give Import Results,
BIOMERO.importer, metadata maintenance in Init, and, where practical, Get
Results one versioned provenance entrance. The navigation and FAIR examples
below are capability requirements for that entrance. They are not part of the
current bug-fix implementation.

## Current divergence

All metadata CSV files are produced by BIOMERO code. They are snapshots of the
workflow and task aggregate state available at export time; they are not a copy
of the complete event stream.

The current consumers nevertheless behave differently:

| Path | Source and timing | OMERO projection |
| --- | --- | --- |
| Import Results | Builds `metadata.csv` after the importer finishes | Independently renders `biomero/workflow`, task, and job maps from the event store |
| BIOMERO.importer | Reads a colocated `metadata.csv` when it imports a file | Flattens all rows into one `biomero.import` map with `csv_` prefixes |
| Init metadata refresh | Reads existing workflow maps and the event store | Updates existing maps, but cannot currently create a complete missing set |
| Get Results | Independently builds a UUID-named CSV after extraction | Uses direct OMERO import/attachment code and its own metadata handling |

The ordering in Import Results means its initial importer run cannot see the
CSV. A later re-import of the same `.analyzed` directory can see it, but creates
a different metadata view. Large values can also make the importer's single
flattened map fail as a unit.

## Design

### One durable record, multiple versioned views

`biomero-schema` should own an OMERO-independent provenance model and the
translations to and from supported serializations:

```text
BIOMERO event-store aggregates
              |
              v
versioned BIOMERO run record <--> metadata.csv
              |
              +--> biomero/workflow searchable view
              +--> future analysis-FAIR profile
              +--> future RO-Crate/Bioschemas export
```

The model is called a run record in this plan. A snapshot is an instance of
that model at specified workflow and task aggregate versions. It is not a
second file or source of truth.

The run-record format and rendered profiles must be versioned independently:

- `biomero-run/v0`, `biomero-run/v1`, and so on describe the durable record.
- `biomero-workflow/v0`, `analysis-fair/v1`, and so on describe derived views.

This allows a representation change without also changing the scientific
metadata profile, and allows a new FAIR profile to be generated from old run
records without rewriting their original evidence.

### Shared package boundary

Both BIOMERO core and BIOMERO.importer already depend on `biomero-schema`.
Putting the neutral model and translations there avoids an importer-to-BIOMERO
dependency cycle. The package must not import OMERO gateway classes, database
sessions, or web configuration.

The initial public API should be conceptually equivalent to:

```python
record = read_provenance_csv(path)       # legacy v0 or an explicit later version
write_provenance_csv(record, path)
views = render_provenance(record, profile="biomero-workflow", version="v0")
```

Each rendered view contains a namespace and ordered key/value entries. OMERO
clients remain responsible for creating, linking, updating, or unlinking the
actual annotations.

### Preserve facts separately from display

The durable record should preserve, when available:

- workflow, task, job, and batch identifiers;
- the workflow and every retained task aggregate version;
- original timestamps and statuses;
- typed selected parameter values;
- input and output object references;
- workflow descriptor, source repository, revision, release, and container
  references;
- commands, environment, logs, and result artifacts;
- import and storage evidence; and
- the original parameter definition or a stable reference and digest for it.

Searchable MapAnnotations are rebuildable views. A view may replace a value
that OMERO cannot index with a reference to the complete attached record, its
UTF-8 size, and its digest. Reduction never removes the value from the durable
record.

### References and links

References should be represented as identities and relations rather than only
as display strings. Examples include input and output OMERO objects, parent and
child batch runs, GitHub repositories and commits, container images and
digests, ontology concepts, logs, and provenance files.

Host-specific URLs are produced by a link resolver supplied by the consuming
application. The durable record retains an OMERO object type and ID or an
external URI. It does not hard-code the current OMERO hostname.

This permits a later OMERO view to expose navigable values such as input and
result Plates, workflow source, Docker Hub artifacts, and OMERO.biomero's
Previous Runs page. OMERO.biomero still needs deep-linkable React state before
workflow history and replay links can be emitted. Those UI routes are future
work and are not required for the alignment implementation.

### Historical compatibility

Existing two-column BIOMERO 2 CSV files are the implicit `biomero-run/v0`
input format and remain readable. The reader should retain unknown fields and
report missing or inferred information rather than discard it.

An upgrade must be additive:

1. Read the original record.
2. Recover additional facts from the event store when it is available.
3. Record the source format, hashes, transformation version, and warnings.
4. Produce a new derived representation or searchable view.
5. Keep the original provenance file attached.

Historical evidence must not be silently rewritten to look as if a newer
schema existed when the analysis ran.

## Integration paths

### Import Results

1. Build the run record after extraction and before upload orders are created.
2. Write the colocated `metadata.csv` before the importer starts.
3. Pass the same record/version through the importer order when useful, while
   keeping the file authoritative for later manual re-imports.
4. Attach the complete file to the actual result Plate or result container.
5. Reconcile the rendered maps after import so target-specific storage and
   output references can be added.
6. Report complete, reduced, and failed annotations accurately.

### BIOMERO.importer

1. Detect and parse a supported BIOMERO run record through `biomero-schema`.
2. Keep generic import tracking in `biomero.import`.
3. Render BIOMERO workflow/task/job maps through the shared profile instead of
   flattening all provenance into `csv_*` keys.
4. Apply annotations independently so one rejected map does not block others.
5. Preserve legacy generic CSV flattening for files that are not recognized as
   BIOMERO provenance.
6. Make initial Import Results imports and later `.analyzed` re-imports produce
   the same profile from the same file.

### Init metadata maintenance

1. Use the shared profile renderer for desired map contents.
2. Update existing maps and create missing maps.
3. Prefer aggregate versions recorded in an existing run record or map.
4. When historical versions are incomplete, use the latest internally
   consistent event-store state, record the aggregate versions used, and emit
   an explicit reconstruction warning instead of skipping all metadata.
5. Preserve unrelated namespaces and user-added keys.

### Get Results investigation

Get Results has no importer stage. It extracts results, creates its own
UUID-named CSV, and uses direct OMERO import and attachment operations. The
transport is different, but the provenance does not need a separate schema.

The implementation investigation should verify that Get Results can:

1. construct the same run record through `biomero-schema`;
2. render the same workflow/task/job profile;
3. attach the complete record before temporary files are cleaned;
4. supply direct-import output references after OMERO objects exist; and
5. retain its current filenames and script options for compatibility.

If all five hold, Get Results becomes another transport adapter using the same
contract. Any unavoidable difference should be documented as a capability
difference, rather than creating a separate metadata format. Its lack of an
importer is not itself a reason to keep a legacy provenance model.

## Error semantics

- Creating the durable run record is independent of optional ZIP and file
  output settings.
- Failure to create or attach the complete provenance file is reported as an
  incomplete provenance outcome.
- Each MapAnnotation is attempted independently.
- Index-size rejection triggers a deterministic reduced view linked to the
  complete record.
- Logs must never report metadata success after the corresponding write failed.
- Pixel import success and provenance completeness are reported separately.

## Delivery sequence

1. **Current scripts fix:** always attach the CSV, reduce oversized maps, isolate
   annotation failures, and report the actual result.
2. **`biomero-schema` contract:** add the v0 reader, versioned run-record model,
   CSV writer, profile renderer, preservation of unknown fields, and migration
   diagnostics.
3. **BIOMERO core:** construct records from pinned aggregate versions and make
   the existing provenance renderer an adapter to the shared profile.
4. **Import Results:** create the CSV before importer submission and reconcile
   the shared view after object creation.
5. **BIOMERO.importer:** recognize BIOMERO records, keep generic import metadata
   separate, and render the shared profile for initial and repeated imports.
6. **Init:** create missing annotations and support explicitly reported latest
   reconstruction when exact historical task revisions are unavailable.
7. **Get Results:** adopt the shared record and renderer after confirming the
   direct-import lifecycle above.

Package releases and dependent PRs should follow that order so no deployed
consumer requires an unpublished schema version. During rollout, consumers
must continue to accept the existing unversioned CSV.

## Acceptance criteria

- A new Import Results run and a later import of its `.analyzed` directory
  produce equivalent `biomero/workflow` views.
- Init can recreate an entirely missing workflow/task/job view.
- Get Results produces the same profile wherever it has the same facts.
- Complete provenance remains attached when a searchable value exceeds OMERO's
  database index limit.
- Oversized or otherwise invalid entries do not block independent annotations.
- Existing BIOMERO 2 CSV files remain readable and retain unknown fields.
- Transformations identify their source and target versions and report inferred
  or unavailable data.

## Deferred capabilities that shape the contract

The shared model must leave room for, without implementing in this feature:

- FAIR analysis profiles and mappings to controlled terms;
- REMBI/ISA study context linked to analysis provenance;
- RO-Crate or Bioschemas exports if an appropriate community profile is
  selected;
- IDR-style navigable MapAnnotation values;
- links to source repositories, immutable revisions, workflow descriptors,
  releases, container registries, and container digests; and
- OMERO.biomero deep links for history, status, rerun-on-same-data, and
  reuse-on-different-data actions.

These requirements are why the importer should not remain a generic flattener
for BIOMERO provenance. All consumers need one versioned semantic translation,
with generic CSV behavior retained only for non-BIOMERO metadata.
