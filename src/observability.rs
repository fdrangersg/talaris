//! Opt-in observability types for marked data-pump APIs.
//!
//! The default `pump_data*` path does not construct these values and does not
//! read clocks. Callers opt in by using `pump_data*_marked`.

use std::fmt;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;

const LATENCY_HISTOGRAM_LOWEST_NS: u64 = 1;
const LATENCY_HISTOGRAM_HIGHEST_NS: u64 = 60_000_000_000;
const LATENCY_HISTOGRAM_SIGFIG: u8 = 3;
const PROMETHEUS_QUANTILES: &[(f64, &str)] = &[
    (0.50, "0.5"),
    (0.90, "0.9"),
    (0.95, "0.95"),
    (0.99, "0.99"),
    (0.999, "0.999"),
    (0.9999, "0.9999"),
];

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("latency histogram setup failed: {0}")]
    Histogram(#[from] hdrhistogram::CreationError),
}

/// Deterministic sampling rate for marked observability.
///
/// The unit is basis points: `10_000` means 100%, `1_000` means 10%, and
/// `0` means disabled. Sampling is deterministic by sequence number and does
/// not read random state on the hot path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservabilitySampleRate {
    basis_points: u16,
}

impl Default for ObservabilitySampleRate {
    #[inline]
    fn default() -> Self {
        Self::always()
    }
}

impl ObservabilitySampleRate {
    pub const MAX_BASIS_POINTS: u16 = 10_000;

    #[inline]
    #[must_use]
    pub const fn always() -> Self {
        Self {
            basis_points: Self::MAX_BASIS_POINTS,
        }
    }

    #[inline]
    #[must_use]
    pub const fn never() -> Self {
        Self { basis_points: 0 }
    }

    #[inline]
    #[must_use]
    pub const fn from_basis_points(basis_points: u16) -> Self {
        let basis_points = if basis_points > Self::MAX_BASIS_POINTS {
            Self::MAX_BASIS_POINTS
        } else {
            basis_points
        };
        Self { basis_points }
    }

    #[inline]
    #[must_use]
    pub const fn basis_points(self) -> u16 {
        self.basis_points
    }

    #[inline]
    #[must_use]
    pub(crate) const fn should_sample_sequence(self, sequence: u64) -> bool {
        if self.basis_points == 0 {
            return false;
        }
        if self.basis_points >= Self::MAX_BASIS_POINTS {
            return true;
        }
        let bucket = sequence.wrapping_mul(0x9E37_79B9_7F4A_7C15) % 10_000;
        bucket < self.basis_points as u64
    }
}

/// HdrHistogram-backed latency recorder for marked WebSocket data events.
///
/// The recorder keeps both cumulative and interval histograms. Cumulative
/// histograms cover the connection lifetime; interval histograms can be exported
/// and reset on each scrape.
#[derive(Debug)]
pub struct LatencyHistograms {
    cumulative: LatencyHistogramSet,
    interval: LatencyHistogramSet,
}

impl LatencyHistograms {
    pub fn new() -> Result<Self, ObservabilityError> {
        Ok(Self {
            cumulative: LatencyHistogramSet::new()?,
            interval: LatencyHistogramSet::new()?,
        })
    }

    #[inline]
    pub(crate) fn record_plaintext_chunk(&mut self, meta: DataEventMeta) {
        self.cumulative.record_plaintext_chunk(meta);
        self.interval.record_plaintext_chunk(meta);
    }

    #[inline]
    pub(crate) fn record_message(&mut self, meta: DataEventMeta) {
        self.cumulative.record_message(meta);
        self.interval.record_message(meta);
    }

    pub fn write_prometheus_help<W: fmt::Write>(out: &mut W) -> fmt::Result {
        writeln!(
            out,
            "# HELP talaris_ws_latency_quantile_ns HdrHistogram client-side quantiles for sampled WebSocket data-message latency stages."
        )?;
        writeln!(out, "# TYPE talaris_ws_latency_quantile_ns gauge")?;
        writeln!(
            out,
            "# HELP talaris_ws_latency_samples Sample count for local HdrHistogram latency stages."
        )?;
        writeln!(out, "# TYPE talaris_ws_latency_samples gauge")?;
        writeln!(
            out,
            "# HELP talaris_ws_latency_sum_ns Sum of sampled WebSocket data-message latency in nanoseconds."
        )?;
        writeln!(out, "# TYPE talaris_ws_latency_sum_ns gauge")?;
        writeln!(
            out,
            "# HELP talaris_ws_latency_max_ns Maximum sampled WebSocket data-message latency in nanoseconds."
        )?;
        writeln!(out, "# TYPE talaris_ws_latency_max_ns gauge")
    }

    pub fn write_prometheus_cumulative<W: fmt::Write>(
        &self,
        conn_id: u32,
        out: &mut W,
    ) -> fmt::Result {
        self.cumulative.write_prometheus(conn_id, "cumulative", out)
    }

    pub fn write_prometheus_interval<W: fmt::Write>(
        &self,
        conn_id: u32,
        out: &mut W,
    ) -> fmt::Result {
        self.interval.write_prometheus(conn_id, "interval", out)
    }

    pub fn write_prometheus_interval_and_reset<W: fmt::Write>(
        &mut self,
        conn_id: u32,
        out: &mut W,
    ) -> fmt::Result {
        self.write_prometheus_interval(conn_id, out)?;
        self.interval.reset();
        Ok(())
    }
}

#[derive(Debug)]
struct LatencyHistogramSet {
    recv_to_plaintext: StageHistogram,
    plaintext_to_ws_all: StageHistogram,
    plaintext_to_ws_first: StageHistogram,
    plaintext_to_ws_queued: StageHistogram,
    recv_to_ws_all: StageHistogram,
    recv_to_ws_first: StageHistogram,
    recv_to_ws_queued: StageHistogram,
}

impl LatencyHistogramSet {
    fn new() -> Result<Self, ObservabilityError> {
        Ok(Self {
            recv_to_plaintext: StageHistogram::new()?,
            plaintext_to_ws_all: StageHistogram::new()?,
            plaintext_to_ws_first: StageHistogram::new()?,
            plaintext_to_ws_queued: StageHistogram::new()?,
            recv_to_ws_all: StageHistogram::new()?,
            recv_to_ws_first: StageHistogram::new()?,
            recv_to_ws_queued: StageHistogram::new()?,
        })
    }

    #[inline]
    fn record_plaintext_chunk(&mut self, meta: DataEventMeta) {
        if let Some(nanos) = meta.recv_to_plaintext_nanos() {
            self.recv_to_plaintext.record(nanos);
        }
    }

    #[inline]
    fn record_message(&mut self, meta: DataEventMeta) {
        let position = MessageChunkPosition::from_meta(meta);
        if let Some(nanos) = meta.plaintext_to_ws_nanos() {
            self.plaintext_to_ws_all.record(nanos);
            match position {
                MessageChunkPosition::First => self.plaintext_to_ws_first.record(nanos),
                MessageChunkPosition::Queued => self.plaintext_to_ws_queued.record(nanos),
            }
        }
        if let Some(nanos) = meta.recv_to_ws_nanos() {
            self.recv_to_ws_all.record(nanos);
            match position {
                MessageChunkPosition::First => self.recv_to_ws_first.record(nanos),
                MessageChunkPosition::Queued => self.recv_to_ws_queued.record(nanos),
            }
        }
    }

    fn reset(&mut self) {
        self.recv_to_plaintext.reset();
        self.plaintext_to_ws_all.reset();
        self.plaintext_to_ws_first.reset();
        self.plaintext_to_ws_queued.reset();
        self.recv_to_ws_all.reset();
        self.recv_to_ws_first.reset();
        self.recv_to_ws_queued.reset();
    }

    fn write_prometheus<W: fmt::Write>(
        &self,
        conn_id: u32,
        window: &str,
        out: &mut W,
    ) -> fmt::Result {
        self.recv_to_plaintext.write_prometheus(
            conn_id,
            window,
            "chunk",
            "recv_to_plaintext",
            "chunk",
            out,
        )?;
        self.plaintext_to_ws_all.write_prometheus(
            conn_id,
            window,
            "message",
            "plaintext_to_ws",
            "all",
            out,
        )?;
        self.plaintext_to_ws_first.write_prometheus(
            conn_id,
            window,
            "message",
            "plaintext_to_ws",
            "first",
            out,
        )?;
        self.plaintext_to_ws_queued.write_prometheus(
            conn_id,
            window,
            "message",
            "plaintext_to_ws",
            "queued",
            out,
        )?;
        self.recv_to_ws_all.write_prometheus(
            conn_id,
            window,
            "message",
            "recv_to_ws",
            "all",
            out,
        )?;
        self.recv_to_ws_first.write_prometheus(
            conn_id,
            window,
            "message",
            "recv_to_ws",
            "first",
            out,
        )?;
        self.recv_to_ws_queued.write_prometheus(
            conn_id,
            window,
            "message",
            "recv_to_ws",
            "queued",
            out,
        )
    }
}

#[derive(Clone, Copy, Debug)]
enum MessageChunkPosition {
    First,
    Queued,
}

impl MessageChunkPosition {
    #[inline]
    const fn from_meta(meta: DataEventMeta) -> Self {
        if meta.chunk_message_index == 0 {
            Self::First
        } else {
            Self::Queued
        }
    }
}

#[derive(Debug)]
struct StageHistogram {
    hist: hdrhistogram::Histogram<u64>,
    total_nanos: u64,
}

impl StageHistogram {
    fn new() -> Result<Self, ObservabilityError> {
        Ok(Self {
            hist: hdrhistogram::Histogram::new_with_bounds(
                LATENCY_HISTOGRAM_LOWEST_NS,
                LATENCY_HISTOGRAM_HIGHEST_NS,
                LATENCY_HISTOGRAM_SIGFIG,
            )?,
            total_nanos: 0,
        })
    }

    #[inline]
    fn record(&mut self, nanos: u64) {
        self.hist.saturating_record(nanos.max(1));
        self.total_nanos = self.total_nanos.saturating_add(nanos);
    }

    fn reset(&mut self) {
        self.hist.reset();
        self.total_nanos = 0;
    }

    fn write_prometheus<W: fmt::Write>(
        &self,
        conn_id: u32,
        window: &str,
        scope: &str,
        stage: &str,
        chunk_position: &str,
        out: &mut W,
    ) -> fmt::Result {
        for &(quantile, quantile_label) in PROMETHEUS_QUANTILES {
            let value = if self.hist.is_empty() {
                0
            } else {
                self.hist.value_at_quantile(quantile)
            };
            writeln!(
                out,
                "talaris_ws_latency_quantile_ns{{conn_id=\"{conn_id}\",window=\"{window}\",scope=\"{scope}\",stage=\"{stage}\",chunk_position=\"{chunk_position}\",quantile=\"{quantile_label}\"}} {value}"
            )?;
        }
        writeln!(
            out,
            "talaris_ws_latency_samples{{conn_id=\"{conn_id}\",window=\"{window}\",scope=\"{scope}\",stage=\"{stage}\",chunk_position=\"{chunk_position}\"}} {}",
            self.hist.len()
        )?;
        writeln!(
            out,
            "talaris_ws_latency_sum_ns{{conn_id=\"{conn_id}\",window=\"{window}\",scope=\"{scope}\",stage=\"{stage}\",chunk_position=\"{chunk_position}\"}} {}",
            self.total_nanos
        )?;
        let max = if self.hist.is_empty() {
            0
        } else {
            self.hist.max()
        };
        writeln!(
            out,
            "talaris_ws_latency_max_ns{{conn_id=\"{conn_id}\",window=\"{window}\",scope=\"{scope}\",stage=\"{stage}\",chunk_position=\"{chunk_position}\"}} {max}"
        )
    }
}

/// Per-message transport timing metadata emitted by marked data-pump APIs.
///
/// `source_recv_time_nanos` is a Unix epoch timestamp sampled when the user
/// thread observes the recv CQE. It is suitable for embedding into downstream
/// wire messages if host clocks are synchronized.
///
/// The `*_mono_nanos` fields are process-local monotonic timestamps. They are
/// only meaningful for deltas on the same host/process.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DataEventMeta {
    /// Whether this event carries latency timestamps. Sequence and index fields
    /// remain populated even when sampling skips timestamp reads.
    pub sampled: bool,
    /// Unix epoch nanos **sampled** when the recv CQE is observed by user space.
    pub source_recv_time_nanos: i64,
    /// Per-connection sequence assigned to positive-length recv CQEs observed by
    /// the marked data-pump path. Unmarked pumps do not advance this sequence.
    pub recv_sequence: u64,
    /// Monotonic nanos **sampled** when the recv CQE is observed by user space.
    pub transport_recv_mono_nanos: u64,
    /// Monotonic nanos **sampled** when a TLS plaintext chunk is ready for WS parse.
    /// For plain TCP connections this equals `transport_recv_mono_nanos`.
    pub tls_plaintext_ready_mono_nanos: u64,
    /// Monotonic nanos **sampled** immediately before handing the WS payload to the
    /// user's sink.
    pub ws_payload_ready_mono_nanos: u64,
    /// Per-connection sequence assigned to data messages emitted by the marked
    /// data-pump path. Unmarked pumps do not advance this sequence.
    pub message_sequence: u64,
    /// Index of the TLS plaintext chunk emitted from this recv CQE. Saturates at
    /// `u16::MAX` if a single recv CQE yields more chunks than fit in `u16`.
    pub tls_plaintext_chunk_index: u16,
    /// Index of the WS data message emitted from this plaintext chunk. Saturates
    /// at `u16::MAX` if a single plaintext chunk yields more data messages than
    /// fit in `u16`.
    pub chunk_message_index: u16,
}

impl DataEventMeta {
    #[inline]
    #[must_use]
    pub(crate) fn recv_observed_now(recv_sequence: u64, sampled: bool) -> Self {
        Self {
            sampled,
            source_recv_time_nanos: if sampled { unix_epoch_nanos_now() } else { 0 },
            recv_sequence,
            transport_recv_mono_nanos: if sampled { monotonic_nanos_now() } else { 0 },
            tls_plaintext_ready_mono_nanos: 0,
            ws_payload_ready_mono_nanos: 0,
            message_sequence: 0,
            tls_plaintext_chunk_index: 0,
            chunk_message_index: 0,
        }
    }

    #[inline]
    #[must_use]
    pub(crate) const fn plaintext_ready_at(mut self, mono_nanos: u64, chunk_index: u16) -> Self {
        self.tls_plaintext_ready_mono_nanos = if self.sampled { mono_nanos } else { 0 };
        self.tls_plaintext_chunk_index = chunk_index;
        self.chunk_message_index = 0;
        self
    }

    #[inline]
    #[must_use]
    pub(crate) fn plaintext_ready_now(self, chunk_index: u16) -> Self {
        let mono_nanos = if self.sampled {
            monotonic_nanos_now()
        } else {
            0
        };
        self.plaintext_ready_at(mono_nanos, chunk_index)
    }

    #[inline]
    #[must_use]
    pub(crate) fn ws_payload_ready_now(
        mut self,
        chunk_message_index: u16,
        message_sequence: u64,
    ) -> Self {
        self.ws_payload_ready_mono_nanos = if self.sampled {
            monotonic_nanos_now()
        } else {
            0
        };
        self.message_sequence = message_sequence;
        self.chunk_message_index = chunk_message_index;
        self
    }

    #[inline]
    #[must_use]
    pub fn recv_to_plaintext_nanos(self) -> Option<u64> {
        if !self.sampled {
            return None;
        }
        self.tls_plaintext_ready_mono_nanos
            .checked_sub(self.transport_recv_mono_nanos)
    }

    #[inline]
    #[must_use]
    pub fn plaintext_to_ws_nanos(self) -> Option<u64> {
        if !self.sampled {
            return None;
        }
        self.ws_payload_ready_mono_nanos
            .checked_sub(self.tls_plaintext_ready_mono_nanos)
    }

    #[inline]
    #[must_use]
    pub fn recv_to_ws_nanos(self) -> Option<u64> {
        if !self.sampled {
            return None;
        }
        self.ws_payload_ready_mono_nanos
            .checked_sub(self.transport_recv_mono_nanos)
    }
}

/// Data-only WebSocket event carrying transport timing metadata.
#[derive(Debug)]
pub enum MarkedDataEvent<'a> {
    Text {
        payload: &'a str,
        meta: DataEventMeta,
    },
    Binary {
        payload: &'a [u8],
        meta: DataEventMeta,
    },
}

impl MarkedDataEvent<'_> {
    #[inline]
    #[must_use]
    pub const fn meta(&self) -> DataEventMeta {
        match self {
            Self::Text { meta, .. } | Self::Binary { meta, .. } => *meta,
        }
    }
}

#[inline]
pub(crate) fn monotonic_nanos_now() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    let nanos = start.elapsed().as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

#[inline]
fn unix_epoch_nanos_now() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_rate_respects_boundaries() {
        let never = ObservabilitySampleRate::never();
        assert!(!never.should_sample_sequence(0));
        assert!(!never.should_sample_sequence(u64::MAX));

        let always = ObservabilitySampleRate::always();
        assert!(always.should_sample_sequence(0));
        assert!(always.should_sample_sequence(u64::MAX));

        let saturated = ObservabilitySampleRate::from_basis_points(10_001);
        assert_eq!(
            saturated.basis_points(),
            ObservabilitySampleRate::MAX_BASIS_POINTS
        );
    }

    #[test]
    fn unsampled_meta_does_not_report_deltas() {
        let meta = DataEventMeta::recv_observed_now(7, false)
            .plaintext_ready_now(0)
            .ws_payload_ready_now(0, 11);

        assert!(!meta.sampled);
        assert_eq!(meta.recv_sequence, 7);
        assert_eq!(meta.message_sequence, 11);
        assert_eq!(meta.source_recv_time_nanos, 0);
        assert_eq!(meta.transport_recv_mono_nanos, 0);
        assert_eq!(meta.tls_plaintext_ready_mono_nanos, 0);
        assert_eq!(meta.ws_payload_ready_mono_nanos, 0);
        assert_eq!(meta.recv_to_plaintext_nanos(), None);
        assert_eq!(meta.plaintext_to_ws_nanos(), None);
        assert_eq!(meta.recv_to_ws_nanos(), None);
    }

    #[test]
    fn latency_histograms_export_prometheus() -> Result<(), Box<dyn std::error::Error>> {
        let mut histograms = LatencyHistograms::new()?;
        let first = DataEventMeta {
            sampled: true,
            source_recv_time_nanos: 1,
            recv_sequence: 2,
            transport_recv_mono_nanos: 100,
            tls_plaintext_ready_mono_nanos: 160,
            ws_payload_ready_mono_nanos: 250,
            message_sequence: 3,
            tls_plaintext_chunk_index: 0,
            chunk_message_index: 0,
        };
        let queued = DataEventMeta {
            chunk_message_index: 1,
            message_sequence: 4,
            ws_payload_ready_mono_nanos: 330,
            ..first
        };
        histograms.record_plaintext_chunk(first);
        histograms.record_message(first);
        histograms.record_message(queued);

        let mut out = String::new();
        LatencyHistograms::write_prometheus_help(&mut out)?;
        histograms.write_prometheus_cumulative(7, &mut out)?;

        assert!(out.contains("# TYPE talaris_ws_latency_quantile_ns gauge"));
        assert!(out.contains(
            "talaris_ws_latency_samples{conn_id=\"7\",window=\"cumulative\",scope=\"chunk\",stage=\"recv_to_plaintext\",chunk_position=\"chunk\"} 1"
        ));
        assert!(out.contains(
            "talaris_ws_latency_sum_ns{conn_id=\"7\",window=\"cumulative\",scope=\"chunk\",stage=\"recv_to_plaintext\",chunk_position=\"chunk\"} 60"
        ));
        assert!(out.contains(
            "talaris_ws_latency_samples{conn_id=\"7\",window=\"cumulative\",scope=\"message\",stage=\"plaintext_to_ws\",chunk_position=\"all\"} 2"
        ));
        assert!(
            out.contains("talaris_ws_latency_sum_ns{conn_id=\"7\",window=\"cumulative\",scope=\"message\",stage=\"plaintext_to_ws\",chunk_position=\"queued\"} 170")
        );
        assert!(
            out.contains("talaris_ws_latency_max_ns{conn_id=\"7\",window=\"cumulative\",scope=\"message\",stage=\"recv_to_ws\",chunk_position=\"all\"} 230")
        );
        Ok(())
    }

    #[test]
    fn interval_export_resets_interval_histograms() -> Result<(), Box<dyn std::error::Error>> {
        let mut histograms = LatencyHistograms::new()?;
        let meta = DataEventMeta {
            sampled: true,
            source_recv_time_nanos: 1,
            recv_sequence: 2,
            transport_recv_mono_nanos: 100,
            tls_plaintext_ready_mono_nanos: 160,
            ws_payload_ready_mono_nanos: 250,
            message_sequence: 3,
            tls_plaintext_chunk_index: 0,
            chunk_message_index: 0,
        };
        histograms.record_plaintext_chunk(meta);
        histograms.record_message(meta);

        let mut first = String::new();
        histograms.write_prometheus_interval_and_reset(9, &mut first)?;
        assert!(first.contains(
            "talaris_ws_latency_samples{conn_id=\"9\",window=\"interval\",scope=\"message\",stage=\"recv_to_ws\",chunk_position=\"all\"} 1"
        ));

        let mut second = String::new();
        histograms.write_prometheus_interval(9, &mut second)?;
        assert!(second.contains(
            "talaris_ws_latency_samples{conn_id=\"9\",window=\"interval\",scope=\"message\",stage=\"recv_to_ws\",chunk_position=\"all\"} 0"
        ));

        let mut cumulative = String::new();
        histograms.write_prometheus_cumulative(9, &mut cumulative)?;
        assert!(cumulative.contains(
            "talaris_ws_latency_samples{conn_id=\"9\",window=\"cumulative\",scope=\"message\",stage=\"recv_to_ws\",chunk_position=\"all\"} 1"
        ));
        Ok(())
    }
}
