---
name: bioagent-w1-read-qc
description: Compile and run BioAgent Workflow 1 read quality control through the governed fastp capability; use for default or controlled read-QC preset changes, never for arbitrary shell execution.
---

# BioAgent W1 read QC

Use the `read_qc` stage with paired FASTQ input. Resolve `fastp` from the local tool
registry, require the manifest version, and compile through
`workflow1.read_qc.fastp`; do not construct or accept a raw command.

Parameter changes must use the published manifest fields and create a new immutable
execution specification. Preserve the source FASTQ files, write outputs only inside
the run root, and return the structured tool result recorded by the worker.

The default preset follows Workflow 1. A controlled change may override only the
listed parameters in `manifest.json`; paths, output locations, threads, and raw CLI
fragments are not skill parameters.
