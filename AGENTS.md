# Agent instructions for RAX

RAX is a Rust port of AX (https://github.com/google/ax). These rules apply to
every agent working in this repository.

## Hard rules

- **Never mention or reference upstream AX pull requests or issues directly.**
  That covers PR and issue numbers (`#123`, `google/ax#123`, "AX PR 123") and
  links to them (`github.com/google/ax/pull/...`, `.../issues/...`), in RAX
  pull requests, commit messages, code comments, docs, review replies and any
  other message. Mentions like these notify the upstream project and create
  cross-references there. Describe the upstream change in your own words
  instead, for example "as AX now does" or "AX's CreateTask now rejects
  missing workspaces". Naming an AX commit hash to pin a parity point (as the
  README does) is fine.
