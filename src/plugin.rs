//! Extensible segment component plugin system.
//!
//! This module defines the [`SegmentPlugin`] trait and supporting types that allow
//! custom data structures to participate in the segment lifecycle (write, read, merge).
//!
//! The built-in components (postings, fast fields, field norms, store) are themselves
//! implemented as plugins; external code attaches new data to segments through the same
//! trait without modifying tantivy internals.

use std::any::Any;
use std::collections::BTreeMap;

use common::HasLen;

use crate::index::{IndexSettings, SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::SegmentDocIdMapping;
use crate::schema::document::ErasedDocument;
use crate::schema::Schema;
use crate::space_usage::ComponentSpaceUsage;
use crate::{DocId, Segment};

/// A pluggable segment component that participates in writing and merging.
///
/// Each plugin manages one or more files within a segment. The plugin is a factory
/// that creates writers and handles merging. The actual data APIs are
/// component-specific and accessed via downcasting on the concrete types.
///
/// # Ordering
///
/// Plugins are written and merged in [`Index::all_plugins`] order: the built-ins first
/// (field norms, postings, fast fields, store — postings reads field norms back from
/// disk, so field norms come first), then custom plugins in the order they were
/// registered. A plugin can read any earlier plugin's output from disk; custom plugins
/// run after all built-ins, so they can read built-in output, and a built-in never
/// depends on a custom plugin.
///
/// [`Index::all_plugins`]: crate::Index::all_plugins
pub trait SegmentPlugin: Send + Sync + 'static {
    /// File extensions this component manages (e.g., `["idx", "pos", "term"]` for postings).
    fn extensions(&self) -> &[&str];

    /// Create a writer for accumulating and serializing data during indexing.
    ///
    /// Returns a type-erased writer. The `SegmentWriter` will downcast to the concrete
    /// type when it needs to call component-specific APIs (e.g., feeding terms to
    /// the postings writer).
    fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>>;

    /// Merge data from multiple source segments into a target segment.
    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()>;

    /// Report on-disk space usage of this component, keyed by component name.
    ///
    /// The returned entries are merged into [`SegmentSpaceUsage`]. The default
    /// implementation emits one [`ComponentSpaceUsage::Basic`] entry per file in
    /// [`extensions()`](Self::extensions); built-in plugins override this to report
    /// richer per-field breakdowns under the keys the named accessors expect.
    ///
    /// [`SegmentSpaceUsage`]: crate::space_usage::SegmentSpaceUsage
    fn space_usage(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<BTreeMap<String, ComponentSpaceUsage>> {
        let mut usage = BTreeMap::new();
        for &ext in self.extensions() {
            let file = segment_reader.open_read(SegmentComponent::Custom(ext.to_string()))?;
            usage.insert(
                ext.to_string(),
                ComponentSpaceUsage::Basic(file.len().into()),
            );
        }
        Ok(usage)
    }
}

/// Writer for a single component within a segment.
///
/// The writer accumulates data during indexing (via [`add_document`](Self::add_document)
/// and component-specific APIs on the concrete type) and serializes it to segment files
/// during finalization.
pub trait PluginWriter: Send + Any {
    /// Records a single document during indexing.
    ///
    /// Called once per document added to the segment, in doc-id order, for every custom
    /// plugin writer. The document is passed type-erased as [`&dyn ErasedDocument`] — custom
    /// plugins are trait objects, so they cannot take a generic `Document`. A plugin that
    /// knows the concrete document type can recover it for free with
    /// `doc.as_any().downcast_ref::<ConcreteDoc>()`; otherwise it can walk `doc.erased_fields()`.
    /// The default is a no-op; override it to accumulate per-document state.
    fn add_document(
        &mut self,
        _doc_id: DocId,
        _doc: &dyn ErasedDocument,
        _schema: &Schema,
    ) -> crate::Result<()> {
        Ok(())
    }

    /// Serialize accumulated data to segment files and finalize them.
    /// Called once, during `SegmentWriter::finalize()`.
    ///
    /// Consumes the writer: it is the terminal step of the writer's lifecycle, responsible
    /// for both writing the segment files and closing their handles.
    fn serialize(
        self: Box<Self>,
        segment: &Segment,
        doc_id_map: Option<&crate::indexer::doc_id_mapping::DocIdMapping>,
    ) -> crate::Result<()>;

    /// Current memory usage of this writer.
    fn mem_usage(&self) -> usize;

    // Downcast support for accessing component-specific APIs. Once the crate MSRV
    // reaches Rust 1.86, these can be dropped: trait upcasting lets callers coerce
    // `&dyn PluginWriter` to `&dyn Any` and call `downcast_ref`/`downcast_mut` directly.
    /// Returns this writer as [`Any`] for immutable downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns this writer as [`Any`] for mutable downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Context provided to [`SegmentPlugin::create_writer`].
///
/// The schema, settings, and directory are reachable from `segment`
/// (`segment.schema()`, `segment.index().settings()`, `segment.index().directory()`).
pub struct PluginWriterContext<'a> {
    /// The segment being written to.
    pub segment: &'a Segment,
    /// Per-thread indexing memory budget in bytes. Plugins that keep an in-memory arena
    /// (e.g. the inverted index) size it from this.
    pub memory_budget_in_bytes: usize,
}

/// Context provided to [`SegmentPlugin::merge`].
pub struct PluginMergeContext<'a> {
    /// Readers for the source segments being merged.
    pub readers: &'a [SegmentReader],
    /// Mapping from target document IDs to source segment document IDs.
    pub doc_id_mapping: &'a SegmentDocIdMapping,
    /// Segment that receives the merged plugin data.
    pub target_segment: &'a Segment,
    /// Schema of the target index.
    pub schema: &'a Schema,
    /// Settings of the target index.
    pub settings: &'a IndexSettings,
}

#[cfg(test)]
mod tests {
    //! Round-trip integration test for the segment plugin system.
    //!
    //! Defines a custom marker plugin, then verifies it works through the
    //! full lifecycle: write → read → merge → read.

    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::index::SegmentComponent;
    use crate::schema::document::{ErasedDocument, Value};
    use crate::schema::{Field, FieldType, Schema, STORED, TEXT};
    use crate::{Index, IndexWriter, TantivyDocument};

    const MARKER: u32 = 0xDEADBEEF;

    /// A simple plugin that writes a fixed marker to a custom file, and — when the schema
    /// declares a custom field — also persists that field's opaque payloads. This exercises the
    /// plugin-defined field type end to end: the schema binds a type name, and the plugin
    /// consumes the values by matching that field, with no other coupling.
    struct MarkerPlugin;

    impl SegmentPlugin for MarkerPlugin {
        fn extensions(&self) -> &[&str] {
            &["marker"]
        }

        fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
            // Find the (optional) custom field this plugin should consume.
            let custom_field = ctx.segment.schema().fields().find_map(|(field, entry)| {
                matches!(entry.field_type(), FieldType::Custom(_)).then_some(field)
            });
            Ok(Box::new(MarkerWriter {
                custom_field,
                payloads: Vec::new(),
            }))
        }

        fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
            let component = SegmentComponent::Custom("marker".to_string());
            let mut write = ctx.target_segment.open_write(component)?;
            use std::io::Write;
            write.write_all(&MARKER.to_le_bytes())?;
            common::TerminatingWrite::terminate(write)?;
            Ok(())
        }
    }

    /// The marker file is `MARKER` followed by each custom payload as `[u32 len][bytes]`.
    fn parse_marker_payloads(data: &[u8]) -> Vec<Vec<u8>> {
        assert_eq!(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            MARKER
        );
        let mut payloads = Vec::new();
        let mut pos = 4;
        while pos < data.len() {
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            pos += 4;
            payloads.push(data[pos..pos + len].to_vec());
            pos += len;
        }
        payloads
    }

    struct MarkerWriter {
        custom_field: Option<Field>,
        payloads: Vec<Vec<u8>>,
    }

    impl PluginWriter for MarkerWriter {
        fn add_document(
            &mut self,
            _doc_id: DocId,
            doc: &dyn ErasedDocument,
            _schema: &Schema,
        ) -> crate::Result<()> {
            let Some(custom_field) = self.custom_field else {
                return Ok(());
            };
            // This plugin only handles `TantivyDocument`s, so recover the concrete type for free
            // (zero-cost downcast) and read the custom field through its typed API rather than
            // walking the erased document.
            let doc = doc
                .as_any()
                .downcast_ref::<TantivyDocument>()
                .expect("MarkerPlugin only supports TantivyDocument");
            for value in doc.get_all(custom_field) {
                if let Some(bytes) = value.as_custom() {
                    self.payloads.push(bytes.to_vec());
                }
            }
            Ok(())
        }

        fn serialize(
            self: Box<Self>,
            segment: &Segment,
            _doc_id_map: Option<&crate::indexer::doc_id_mapping::DocIdMapping>,
        ) -> crate::Result<()> {
            let component = SegmentComponent::Custom("marker".to_string());
            let mut write = segment.open_write(component)?;
            use std::io::Write;
            write.write_all(&MARKER.to_le_bytes())?;
            for payload in &self.payloads {
                write.write_all(&(payload.len() as u32).to_le_bytes())?;
                write.write_all(payload)?;
            }
            common::TerminatingWrite::terminate(write)?;
            Ok(())
        }

        fn mem_usage(&self) -> usize {
            self.payloads.iter().map(Vec::len).sum()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn test_plugin_full_lifecycle() -> crate::Result<()> {
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        // A plugin-defined field type. Only the type name is declared here; no plugin is named.
        let payload_field = schema_builder.add_custom_field("payload", "marker_payload", json!({}));
        let schema = schema_builder.build();

        let plugin: Arc<dyn SegmentPlugin> = Arc::new(MarkerPlugin);
        let index = Index::builder()
            .schema(schema)
            .register_plugin(plugin)
            .create_in_ram()?;

        assert!(index.all_plugins().count() >= 2);
        assert!(
            index
                .all_plugins()
                .any(|plugin| plugin.extensions().contains(&"marker")),
            "marker plugin should be registered"
        );
        assert!(
            index
                .all_plugins()
                .any(|plugin| plugin.extensions().contains(&"fieldnorm")),
            "fieldnorms built-in plugin should be registered"
        );

        // write: two commits, no auto-merge, so we get two distinct segments. Each document
        // carries a custom payload the plugin consumes.
        let add = |writer: &mut IndexWriter, text: &str, payload: &[u8]| {
            let mut doc = TantivyDocument::new();
            doc.add_text(text_field, text);
            doc.add_custom(payload_field, payload);
            writer.add_document(doc).unwrap();
        };
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        add(&mut writer, "hello world", b"p-hello");
        add(&mut writer, "foo bar", b"p-foo");
        writer.commit()?;
        add(&mut writer, "baz qux", b"p-baz");
        writer.commit()?;

        // read: each segment carries the marker plus the custom payloads, both written by
        // MarkerWriter::serialize.
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.num_docs(), 3);
        let segment_readers = searcher.segment_readers();
        assert_eq!(segment_readers.len(), 2);
        let mut all_payloads: Vec<Vec<u8>> = Vec::new();
        for segment_reader in segment_readers {
            let data = segment_reader
                .open_read(SegmentComponent::Custom("marker".to_string()))?
                .read_bytes()?;
            all_payloads.extend(parse_marker_payloads(&data));
        }
        all_payloads.sort();
        assert_eq!(
            all_payloads,
            vec![b"p-baz".to_vec(), b"p-foo".to_vec(), b"p-hello".to_vec()]
        );

        // The built-in store still works for the ordinary field alongside the custom one.
        let stored: TantivyDocument = searcher.doc(crate::DocAddress::new(0, 0))?;
        assert_eq!(
            stored.get_first(text_field).and_then(|v| v.as_str()),
            Some("hello world")
        );

        // merge: exercises MarkerPlugin::merge.
        writer.merge(&index.searchable_segment_ids()?).wait()?;

        // read: the merged segment carries the marker written by MarkerPlugin::merge.
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.num_docs(), 3);
        let segment_readers = searcher.segment_readers();
        assert_eq!(segment_readers.len(), 1);
        let data = segment_readers[0]
            .open_read(SegmentComponent::Custom("marker".to_string()))?
            .read_bytes()?;
        assert_eq!(
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            MARKER
        );

        Ok(())
    }

    #[test]
    fn test_parse_json_rejects_custom_field() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_custom_field("embedding", "vec", json!({ "dim": 3 }));
        let schema = schema_builder.build();

        let err = TantivyDocument::parse_json(&schema, r#"{"embedding": [1.0, 2.0, 3.0]}"#)
            .expect_err("custom fields cannot be populated from JSON");
        assert!(
            format!("{err:?}").contains("custom"),
            "expected a custom-type parse error, got {err:?}"
        );
    }

    #[test]
    fn test_custom_field_schema_roundtrip() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_custom_field("embedding", "vec", json!({ "dim": 3 }));
        let schema = schema_builder.build();

        let reparsed: Schema =
            serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
        let field = reparsed.get_field("embedding").unwrap();
        match reparsed.get_field_entry(field).field_type() {
            FieldType::Custom(options) => {
                assert_eq!(options.type_name(), "vec");
                assert_eq!(options.params()["dim"].as_u64(), Some(3));
            }
            other => panic!("expected a custom field type, got {other:?}"),
        }
    }

    #[test]
    fn test_plugin_extensions() {
        let plugin = MarkerPlugin;
        assert_eq!(plugin.extensions(), &["marker"]);
    }

    #[test]
    fn test_reopen_without_plugin_fails_closed() -> crate::Result<()> {
        use crate::directory::RamDirectory;
        use crate::TantivyError;

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();

        // Build an index with the custom plugin and persist a segment.
        let dir = RamDirectory::create();
        let plugin: Arc<dyn SegmentPlugin> = Arc::new(MarkerPlugin);
        let index = Index::builder()
            .schema(schema)
            .register_plugin(plugin)
            .create(dir.clone())?;
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
            writer.add_document(crate::doc!(text_field => "hello world"))?;
            writer.commit()?;
        }

        // The index records, index-wide, that it requires the "marker" extension.
        let segment_metas = index.searchable_segment_metas()?;
        assert_eq!(segment_metas.len(), 1);
        assert_eq!(
            index.load_metas()?.persisted_custom_extensions,
            vec!["marker".to_string()]
        );

        // Reopen without re-registering the plugin: writing must fail closed
        // rather than silently dropping the plugin's data.
        let reopened = Index::open(dir.clone())?;
        let err = reopened
            .writer_with_num_threads::<crate::TantivyDocument>(1, 15_000_000)
            .err()
            .expect("writer creation should fail when the plugin is not registered");
        assert!(
            matches!(err, TantivyError::MissingPlugin(ref exts) if exts.contains("marker")),
            "expected MissingPlugin error, got {err:?}"
        );

        // Re-registering the plugin clears the guard.
        let mut reopened = reopened;
        reopened.register_plugin(Arc::new(MarkerPlugin));
        let _writer: IndexWriter = reopened.writer_with_num_threads(1, 15_000_000)?;

        Ok(())
    }

    #[test]
    fn test_add_plugin_to_non_empty_index_fails_closed() -> crate::Result<()> {
        use crate::directory::RamDirectory;
        use crate::TantivyError;

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);

        // Build an index WITHOUT the plugin and persist a segment.
        let dir = RamDirectory::create();
        let index = Index::builder()
            .schema(schema_builder.build())
            .create(dir.clone())?;
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
            writer.add_document(crate::doc!(text_field => "hello world"))?;
            writer.commit()?;
        }

        // Registering the plugin now — after the index has data — must fail closed: the
        // existing segment has no "marker" component, so the plugin set can't change.
        let mut reopened = Index::open(dir)?;
        reopened.register_plugin(Arc::new(MarkerPlugin));
        let err = reopened
            .writer_with_num_threads::<crate::TantivyDocument>(1, 15_000_000)
            .err()
            .expect("writer creation should fail when a plugin is added to a non-empty index");
        assert!(
            matches!(err, TantivyError::UnexpectedPlugin(ref exts) if exts.contains("marker")),
            "expected UnexpectedPlugin error, got {err:?}"
        );

        Ok(())
    }

    #[test]
    fn test_conflicting_plugins_fail_closed() -> crate::Result<()> {
        use crate::TantivyError;

        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();

        // Two plugins claiming the same "marker" extension would make their writers
        // contend for the same segment file.
        let index = Index::builder()
            .schema(schema)
            .register_plugin(Arc::new(MarkerPlugin))
            .register_plugin(Arc::new(MarkerPlugin))
            .create_in_ram()?;

        let err = index
            .writer_with_num_threads::<crate::TantivyDocument>(1, 15_000_000)
            .err()
            .expect("writer creation should fail when two plugins claim one extension");
        assert!(
            matches!(err, TantivyError::ConflictingPlugins(ref exts) if exts.contains("marker")),
            "expected ConflictingPlugins error, got {err:?}"
        );
        Ok(())
    }

    #[test]
    fn test_custom_plugin_indexes_custom_document() -> crate::Result<()> {
        use std::collections::BTreeMap;

        use crate::indexer::operation::AddOperation;
        use crate::indexer::SegmentWriter;
        use crate::schema::{Field, OwnedValue};

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let index = Index::builder()
            .schema(schema_builder.build())
            .register_plugin(Arc::new(MarkerPlugin))
            .create_in_ram()?;

        let segment = index.new_segment();
        let mut segment_writer = SegmentWriter::for_segment(15_000_000, segment)?;

        // A custom (non-`TantivyDocument`) document type indexes fine even with a custom plugin
        // registered: built-ins take it generically, and the custom plugin receives it
        // type-erased (`&dyn ErasedDocument`) — no materialization, no error.
        let mut document: BTreeMap<Field, OwnedValue> = BTreeMap::new();
        document.insert(text_field, "hello".into());
        segment_writer.add_document(AddOperation {
            opstamp: 0,
            document,
        })?;

        Ok(())
    }

    #[test]
    fn test_reserved_extension_plugins_fail_closed() -> crate::Result<()> {
        use crate::TantivyError;

        struct ReservedExtPlugin(&'static str);

        impl SegmentPlugin for ReservedExtPlugin {
            fn extensions(&self) -> &[&str] {
                std::slice::from_ref(&self.0)
            }

            fn create_writer(
                &self,
                _ctx: &PluginWriterContext,
            ) -> crate::Result<Box<dyn PluginWriter>> {
                unreachable!("guard rejects the reserved extension before writer creation")
            }

            fn merge(&self, _ctx: PluginMergeContext) -> crate::Result<()> {
                unreachable!("guard rejects the reserved extension before merge")
            }
        }

        // The temp store (`store.temp`) and delete bitset (`del`) are not owned by any
        // plugin, so a custom plugin claiming one would contend for the reserved file.
        for reserved in ["temp", "store.temp", "del"] {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("text", TEXT | STORED);
            let index = Index::builder()
                .schema(schema_builder.build())
                .register_plugin(Arc::new(ReservedExtPlugin(reserved)))
                .create_in_ram()?;

            let err = index
                .writer_with_num_threads::<crate::TantivyDocument>(1, 15_000_000)
                .err()
                .unwrap_or_else(|| {
                    panic!("writer creation should fail when a plugin claims `{reserved}`")
                });
            assert!(
                matches!(err, TantivyError::ConflictingPlugins(ref exts) if exts.contains(reserved)),
                "expected ConflictingPlugins error for `{reserved}`, got {err:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_merge_indices_mismatched_plugins_fails_closed() -> crate::Result<()> {
        use crate::directory::RamDirectory;
        use crate::indexer::merge_indices;
        use crate::TantivyError;

        // Two indices with identical schemas but different plugin sets: only the first
        // registers the marker plugin. Merging must fail closed rather than silently drop
        // the marker component (or read a missing one), since the merged output would carry
        // only the first index's plugin set.
        let build = |with_plugin: bool| -> crate::Result<Index> {
            let mut schema_builder = Schema::builder();
            let text_field = schema_builder.add_text_field("text", TEXT | STORED);
            let mut builder = Index::builder().schema(schema_builder.build());
            if with_plugin {
                builder = builder.register_plugin(Arc::new(MarkerPlugin));
            }
            let index = builder.create_in_ram()?;
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
            writer.add_document(crate::doc!(text_field => "hello world"))?;
            writer.commit()?;
            Ok(index)
        };

        let with_marker = build(true)?;
        let without_marker = build(false)?;

        let Err(err) = merge_indices(&[with_marker, without_marker], RamDirectory::create()) else {
            panic!("merge should fail when source indices register different plugin sets");
        };
        assert!(
            matches!(err, TantivyError::InvalidArgument(ref msg) if msg.contains("plugin sets")),
            "expected InvalidArgument about plugin sets, got {err:?}"
        );

        Ok(())
    }
}
