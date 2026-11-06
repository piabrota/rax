# Agent instructions for RAX

RAX is a Rust port of AX. These rules apply to every agent working in this
repository.

## Hard rules

- **Never reference the upstream AX project directly.** Do not mention or
  link its repository, its users or maintainers, its pull requests or its
  issues: no repository paths or URLs, no `@` handles, no PR or issue
  numbers ("#123", "AX PR 123") and no links to them, in RAX pull requests,
  commit messages, code comments, docs, review replies or any other message.
  Mentions like these notify the upstream project and create
  cross-references there. Describe the upstream change in your own words
  instead, for example "as AX now does" or "AX's CreateTask now rejects
  missing workspaces". Naming an AX commit hash as plain text, without a
  link, to pin a parity point (as the README does) is fine. The only
  exceptions are where a URL is needed for the code to work, such as the
  clone URL `hack/local/local.sh` defaults to, and the license attribution
  in `NOTICE`.
