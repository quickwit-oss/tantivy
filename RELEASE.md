# Release a new Tantivy Version

## Steps

1. Identify new packages in workspace since last release
2. Identify changed packages in workspace since last release
3. Bump version in `Cargo.toml` and their dependents for all changed packages
4. Update version of root `Cargo.toml`
5. Publish version starting with leaf nodes
6. Set git tag with new version


In conjucation with `cargo-release` Steps 1-4 (I'm not sure if the change detection works):
Set new packages to version 0.0.0

Replace prev-tag-name
```bash
cargo release --workspace --no-publish -v --prev-tag-name 0.19 --push-remote origin minor --no-tag --execute
```

no-tag or it will create tags for all the subpackages
