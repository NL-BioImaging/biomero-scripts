# Development TODOs

## Optional pixel-reader audit for reused backing Zarr

- An optional diagnostic could compare authoritative backing Zarr pixels with
  OMERO's pixel reader, for investigating reader or registration problems.
- This is not required for canonical trust: reused backing Zarr is authoritative
  by definition. Fresh canonical exports already require pixel comparison.
- If added, validate well/field mapping, retain connection keepalive, and measure
  the additional read/hash cost separately from normal canonical indexing.
