/// Query-time configuration for IVF adaptive probing — the SPANN shape
/// (NeurIPS 2021), defaults aligned with the paper and SPTAG's shipped
/// config.
///
/// Stop condition, evaluated for the NEXT ranked centroid between
/// clusters — so the first cluster is always scanned: stop at the
/// probe-budget ceiling, OR once the `min_candidates` floor is
/// met AND the next centroid breaches the per-metric distance-ratio
/// gate (SPANN eq. 3). The ceiling is checked first — the
/// [`ProbeTermination`](crate::vector::ProbeTermination) attribution
/// contract.
///
/// The ceiling is measured in filter-effective clusters, not raw
/// clusters: a probed cluster consumes budget on an affine map of its
/// filter pass rate — `SKIPPED_CLUSTER_COST` (a small floor, since the
/// gate pre-pass still scans the cluster) when all rows are filtered,
/// `1.0` when none are, in between otherwise. A selective filter
/// therefore probes deeper into the ranked list before the ceiling
/// binds, since the clusters it skips over cost little (but not nothing).
///
/// All defaults are provisional pending real-data benchmarking.
#[derive(Clone, Debug)]
pub struct AdaptiveProbeParams {
    /// SPANN's query-aware dynamic pruning coefficient — a posting list
    /// is searched iff `Dist(q, c) <= (1 + epsilon) * Dist(q, c_closest)`,
    /// on the per-metric distance defined in the backend gate. The
    /// paper uses 0.6 (recall@1-tuned) to 7.0 (recall@10-tuned); SPTAG
    /// ships `MaxDistRatio = 8.0` = `(1 + 7.0)`. Default 7.0,
    /// PROVISIONAL pending our own benchmarks.
    pub epsilon: f32,
    /// Absolute survivor floor. The call site widens this to
    /// `min_candidates.max(top_n + overfetch_margin)`, so a 0 default
    /// still gives a sane `top_n + overfetch_margin` floor.
    pub min_candidates: usize,
    /// Additive over-fetch margin: the resolved survivor floor is
    /// `top_n + overfetch_margin`. Unlike a multiplicative `m × top_n`
    /// floor, an additive margin keeps the over-probe cushion a *fixed*
    /// number of clusters as `top_n` grows, so the `epsilon` needed for a
    /// target recall stays roughly constant across K instead of shrinking
    /// with it. Default 32, PROVISIONAL.
    pub overfetch_margin: usize,
    /// Filter-effective cluster ceiling, expressed as a FRACTION of the
    /// segment's cluster count and resolved per-segment: a segment with
    /// `num_clusters` clusters probes at most `ceil(max_probe_fraction *
    /// num_clusters)` of them (clamped to `[1, num_clusters]`). A fraction
    /// rather than an absolute count because cluster counts vary segment to
    /// segment — an absolute cap over-probes small segments (clamping to a
    /// full scan) and under-probes large ones. Each probed cluster still
    /// consumes budget equal to its filter pass rate (`(rows - filtered) /
    /// rows`), so an unfiltered query probes at most this fraction while a
    /// selective filter probes proportionally more. SPANN Fig. 2: 99% of
    /// SIFT1M queries reach perfect recall@1 within 114 postings, ~1% of a
    /// 1%-centroid-ratio index's clusters. Default 0.01, PROVISIONAL.
    pub max_probe_fraction: f32,
    /// Lower bound on the resolved probe ceiling, applied before the
    /// `num_clusters` clamp — see [`Self::resolved_probe_ceiling`]. Keeps
    /// small segments, where `max_probe_fraction` rounds down to a single
    /// cluster, probing enough to fill the survivor floor. Defaults to
    /// [`MIN_PROBE_CLUSTERS`].
    pub min_probe_clusters: usize,
}

impl Default for AdaptiveProbeParams {
    fn default() -> Self {
        Self {
            epsilon: 7.0,
            min_candidates: 0,
            overfetch_margin: 32,
            max_probe_fraction: 0.01,
            min_probe_clusters: MIN_PROBE_CLUSTERS,
        }
    }
}

pub(crate) const MIN_PROBE_CLUSTERS: usize = 16;

impl AdaptiveProbeParams {
    /// The probe ceiling for a segment with `num_clusters` IVF clusters:
    /// `ceil(max_probe_fraction * num_clusters)`, lifted to at least
    /// `min_probe_clusters` then clamped to `num_clusters` (so a segment
    /// with fewer clusters than the floor just scans them all). A
    /// non-positive fraction is a configuration error, not "no probing".
    pub(crate) fn resolved_probe_ceiling(&self, num_clusters: usize) -> crate::Result<usize> {
        if !(self.max_probe_fraction > 0.0) {
            return Err(crate::TantivyError::InvalidArgument(
                "max_probe_fraction must be greater than 0".to_string(),
            ));
        }
        let ceiling = (self.max_probe_fraction * num_clusters as f32).ceil() as usize;
        Ok(ceiling.max(self.min_probe_clusters).min(num_clusters))
    }
}

#[cfg(test)]
mod tests {
    use super::AdaptiveProbeParams;

    #[test]
    fn probe_ceiling_resolves_against_cluster_count() -> crate::Result<()> {
        let params = AdaptiveProbeParams {
            max_probe_fraction: 0.25,
            ..Default::default()
        };
        // A quarter of the clusters, rounded up — well above the floor.
        assert_eq!(params.resolved_probe_ceiling(1000)?, 250);
        // A fraction resolving below MIN_PROBE_CLUSTERS is lifted to it...
        assert_eq!(
            params.resolved_probe_ceiling(40)?,
            super::MIN_PROBE_CLUSTERS
        );
        // ...but the floor never exceeds the cluster count.
        assert_eq!(params.resolved_probe_ceiling(2)?, 2);
        // A fraction above 1.0 clamps to the cluster count — small segments
        // scan exhaustively.
        let all = AdaptiveProbeParams {
            max_probe_fraction: 2.0,
            ..Default::default()
        };
        assert_eq!(all.resolved_probe_ceiling(10)?, 10);
        Ok(())
    }

    #[test]
    fn non_positive_probe_fraction_errors() {
        for fraction in [0.0, -1.0] {
            let params = AdaptiveProbeParams {
                max_probe_fraction: fraction,
                ..Default::default()
            };
            assert!(params.resolved_probe_ceiling(9).is_err());
        }
    }
}
