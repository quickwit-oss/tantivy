# Release a new Tantivy Version

## Steps

- Identify new packages in workspace since last release
- Identify changed packages in workspace since last release
- Bump version in `Cargo.toml` and their dependents for all changed packages
- Update version of root `Cargo.toml`
- Publish version starting with leaf nodes
- Set git tag with new version
