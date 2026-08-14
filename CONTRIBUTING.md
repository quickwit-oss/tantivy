# Contributing to Tantivy

There are many ways to contribute to Tantivy: code, bug reports, feature
requests, and documentation improvements are all welcome.

## Submitting a PR

Check if your issue is already listed on [GitHub](https://github.com/quickwit-oss/tantivy/issues).
If it isn't, feel free to open one first to discuss the change.

Reference the related issue in your PR description (e.g. `Closes #<issue number>`),
and include a comprehensive commit message.

Feel free to update `CHANGELOG.md` with your contribution.

## Development

Tantivy compiles on stable Rust.

```bash
git clone https://github.com/quickwit-oss/tantivy.git
cd tantivy
cargo test
```

Please make sure `cargo test` and `cargo clippy` pass before submitting your PR.
