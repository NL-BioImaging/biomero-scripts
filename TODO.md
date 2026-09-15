# Development TODOs

## Optional OMERO-versus-Zarr pixel verification for canonical Plates

- Add an opt-in setting to compare each canonical Plate Zarr image node with
  the corresponding Image read through OMERO, using pixel identities and
  semantic checks. Verify the well/field-to-Image mapping explicitly.
- Cover both newly indexed Plates and existing cached canonical records.
  Set `canonicalPixelVerified=true` only after successful comparison; preserve
  unverified state on mismatch or incomplete verification and report the cause.
- Reuse successful verification for the same source generation under the
  canonical cache validity rules. Avoid repeated full-plate reads per workflow.
- Keep the OMERO connection alive throughout the operation and record timings.
- Document the distinction between hashing a managed source Zarr and verifying
  that it agrees with OMERO's pixel reader. Decide remote-helper eligibility
  for authoritative in-place Zarr sources separately; this optional comparison
  must not silently redefine that trust rule.

Currently `build_canonical_plate_source()` hashes Zarr nodes and explicitly
records `canonicalPixelVerified=false`; it has no OMERO comparison option.
Individual-Image canonicalization already performs that comparison. The reason
for omitting it from the Plate path is not documented. Adding it requires an
additional pixel read/hash through OMERO plus correct node mapping and session
handling. Its actual cost needs measurement, especially for large Plates.
