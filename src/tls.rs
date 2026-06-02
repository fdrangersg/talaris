//! TLS adapter —— 包 `rustls::ClientConnection` 成字节驱动的两口
//!
//! rustls 本来就是字节驱动状态机（不需要 async），只是 API 拆得有点细。
//! 这里把 `read_tls` / `process_new_packets` / `reader()` / `writer()` /
//! `write_tls` 五段封装成 `ingest_ciphertext` + `egress_plaintext` 两个调用，
//! 强制双向 drain，避免漏 `process_new_packets` 或漏 drain handshake 回包。
//! ingress plaintext 通过借用 rustls 内部 chunk 的 callback 同步交给 caller，
//! 不再先复制到中间 `Vec`。
//!
//! ALPN 显式声告 `http/1.1` —— 防止 server 协商 HTTP/2（WS upgrade 要 HTTP/1.1）。
//!
//! 配套 [`super::ws::WsClient`] 用：
//! ```text
//! socket recv → tls.ingest_ciphertext(..., |plaintext| ws.feed_recv(plaintext))
//! ws.pending_tx() → tls.egress_plaintext(...) → socket send
//! ```

#![allow(clippy::module_name_repetitions)]

use std::io::{self, BufRead as _};
use std::sync::Arc;
use thiserror::Error;

/// 协商 ALPN 时 client 唯一接受的协议。
const REQUIRED_ALPN: &[u8] = b"http/1.1";
/// TLSPlaintext / TLSCiphertext header: content type + legacy version + payload length.
const TLS_RECORD_HEADER_SIZE: usize = 5;
/// Match rustls' inbound allowance: 16 KiB plaintext plus 2 KiB ciphertext overhead.
/// rustls rejects payload lengths greater than or equal to this value.
const TLS_RECORD_MAX_PAYLOAD_EXCLUSIVE: usize = 16_384 + 2048;
/// One bounded staging allocation per opted-in connection.
const TLS_RECORD_MAX_WIRE_SIZE: usize = TLS_RECORD_HEADER_SIZE + TLS_RECORD_MAX_PAYLOAD_EXCLUSIVE;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("invalid server name: {0}")]
    InvalidServerName(String),
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// Server 协商了一个非 http/1.1 的 ALPN —— 后续 WS upgrade 不可能成功，
    /// 提前关连接比让用户 debug 一堆"莫名其妙的 WS 协议错误"更友好。
    #[error("server negotiated unexpected ALPN: {0:?}")]
    BadAlpn(Vec<u8>),
    #[error("invalid TLS record header: {0}")]
    InvalidRecordHeader(&'static str),
}

/// Diagnostics for the opt-in bounded TLS-record staging experiment.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct TlsRecordStagingStats {
    pub(crate) records: u64,
    pub(crate) direct_records: u64,
    pub(crate) staged_records: u64,
    pub(crate) copied_bytes: u64,
    pub(crate) max_pending_bytes: u64,
}

/// Buffers at most one incomplete TLS record. Complete records already present in the
/// caller's CQE slice bypass `pending` entirely.
#[derive(Debug)]
struct TlsRecordStager {
    pending: Vec<u8>,
    target_len: Option<usize>,
    stats: TlsRecordStagingStats,
}

impl TlsRecordStager {
    fn new() -> Self {
        Self {
            pending: Vec::with_capacity(TLS_RECORD_MAX_WIRE_SIZE),
            target_len: None,
            stats: TlsRecordStagingStats::default(),
        }
    }

    fn ingest<F>(&mut self, mut src: &[u8], mut on_record: F) -> Result<(), TlsError>
    where
        F: FnMut(&[u8]) -> Result<(), TlsError>,
    {
        while !src.is_empty() {
            if self.pending.is_empty() && src.len() >= TLS_RECORD_HEADER_SIZE {
                let record_len = tls_record_wire_len(src)?;
                if src.len() >= record_len {
                    self.stats.records = self.stats.records.saturating_add(1);
                    self.stats.direct_records = self.stats.direct_records.saturating_add(1);
                    on_record(&src[..record_len])?;
                    src = &src[record_len..];
                    continue;
                }
                self.target_len = Some(record_len);
            }

            let target_len = self.target_len.unwrap_or(TLS_RECORD_HEADER_SIZE);
            let needed = target_len.saturating_sub(self.pending.len());
            let copied = needed.min(src.len());
            self.pending.extend_from_slice(&src[..copied]);
            self.stats.copied_bytes = self.stats.copied_bytes.saturating_add(copied as u64);
            self.stats.max_pending_bytes =
                self.stats.max_pending_bytes.max(self.pending.len() as u64);
            src = &src[copied..];

            if self.pending.len() < TLS_RECORD_HEADER_SIZE {
                continue;
            }
            if self.target_len.is_none() {
                self.target_len = Some(tls_record_wire_len(&self.pending)?);
            }
            let Some(target_len) = self.target_len else {
                unreachable!("target set after complete header");
            };
            if self.pending.len() == target_len {
                self.stats.records = self.stats.records.saturating_add(1);
                self.stats.staged_records = self.stats.staged_records.saturating_add(1);
                on_record(&self.pending)?;
                self.pending.clear();
                self.target_len = None;
            }
        }
        Ok(())
    }
}

fn tls_record_wire_len(header: &[u8]) -> Result<usize, TlsError> {
    debug_assert!(header.len() >= TLS_RECORD_HEADER_SIZE);
    let typ = header[0];
    if !matches!(typ, 0x14..=0x18) {
        return Err(TlsError::InvalidRecordHeader("unknown content type"));
    }
    let payload_len = usize::from(u16::from_be_bytes([header[3], header[4]]));
    if typ != 0x17 && payload_len == 0 {
        return Err(TlsError::InvalidRecordHeader(
            "empty non-application-data payload",
        ));
    }
    if payload_len >= TLS_RECORD_MAX_PAYLOAD_EXCLUSIVE {
        return Err(TlsError::InvalidRecordHeader(
            "payload exceeds rustls limit",
        ));
    }
    Ok(TLS_RECORD_HEADER_SIZE + payload_len)
}

/// rustls client 包装
pub struct TlsAdapter {
    conn: rustls::ClientConnection,
    /// `IoState::peer_has_closed` 的最新观察。每次 `process_new_packets` 后刷新。
    /// rustls 0.23 不在 `ClientConnection` 上直接暴露这个 flag —— 它只在
    /// `process_new_packets` 返回的 `IoState` 上。我们 cache 一下。
    peer_closed_notify: bool,
    /// `Some` only for the explicit record-aware staging experiment.
    record_stager: Option<TlsRecordStager>,
}

impl std::fmt::Debug for TlsAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsAdapter")
            .field("is_handshaking", &self.conn.is_handshaking())
            .field("record_staging", &self.record_stager.is_some())
            .finish()
    }
}

impl TlsAdapter {
    /// 构造 client。`server_name` 通常是连接的主机名（用于 SNI + 证书校验）。
    pub fn new_client(server_name: &str) -> Result<Self, TlsError> {
        let root_store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let mut config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        // 显式只接受 HTTP/1.1，避免 server 协商 HTTP/2 后 WS upgrade 不通。
        // 仅"通告"还不够 —— handshake 完后必须再用 [`verify_alpn`] 校验 server
        // 真的回了 http/1.1（或不回）；某些 misconfig server 会忽略 alpn 通告。
        config.alpn_protocols = vec![REQUIRED_ALPN.to_vec()];

        Self::new_client_with_config(server_name, Arc::new(config))
    }

    /// 用 caller 提供的 rustls 配置构造 client。私有 CA、session cache、crypto
    /// provider 或其它 rustls 级调优走这里注入。
    pub fn new_client_with_config(
        server_name: &str,
        config: Arc<rustls::ClientConfig>,
    ) -> Result<Self, TlsError> {
        let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|_| TlsError::InvalidServerName(server_name.to_owned()))?;
        let conn = rustls::ClientConnection::new(config, name)?;
        Ok(Self {
            conn,
            peer_closed_notify: false,
            record_stager: None,
        })
    }

    /// Opt into bounded, record-aware TLS ingress staging. Complete records already present
    /// in one recv CQE remain zero-copy; split records copy into one bounded 18,437-byte buffer.
    #[must_use]
    pub fn with_record_staging(mut self, on: bool) -> Self {
        self.record_stager = on.then(TlsRecordStager::new);
        self
    }

    #[must_use]
    pub(crate) fn record_staging_stats(&self) -> Option<TlsRecordStagingStats> {
        self.record_stager.as_ref().map(|stager| stager.stats)
    }

    /// 是否还在 TLS handshake 阶段
    #[must_use]
    pub fn is_handshaking(&self) -> bool {
        self.conn.is_handshaking()
    }

    /// 校验 ALPN 协商结果。handshake 完成后调一次：
    ///
    /// - `Some(b"http/1.1")` → Ok
    /// - `None` → Ok（server 没参与 ALPN，RFC 7301 允许）
    /// - 其它 → `Err(BadAlpn)`，caller 应立即关连接
    pub fn verify_alpn(&self) -> Result<(), TlsError> {
        match self.conn.alpn_protocol() {
            None => Ok(()),
            Some(p) if p == REQUIRED_ALPN => Ok(()),
            Some(other) => Err(TlsError::BadAlpn(other.to_vec())),
        }
    }

    /// peer 是否已发 close_notify 干净关闭。WS / 应用层收到 true 后应该
    /// 推自己的 state 到 Closing/Closed，停止再喂 ciphertext。
    /// 值由 `process_new_packets` 之后的 `IoState::peer_has_closed` 缓存而来。
    #[must_use]
    pub fn received_close_notify(&self) -> bool {
        self.peer_closed_notify
    }

    /// 在本端排队一个 close_notify alert。下一次 `drain_ciphertext`
    /// （由 `egress_plaintext` / `ingest_ciphertext` 触发）会把它写到
    /// `dst_ciphertext`，caller 负责送到 socket。
    pub fn send_close_notify(&mut self) {
        self.conn.send_close_notify();
    }

    /// 喂从 socket 收到的密文字节；每块可读明文通过 `on_plaintext` 同步借给 caller，
    /// rustls 在 handshake / alert 阶段需要回发的密文 append 到 `dst_ciphertext`
    /// （caller 必须把这部分也 send 回 socket，否则 handshake 卡死）。
    ///
    /// `on_plaintext` 返回后对应 chunk 会立刻从 rustls reader 消费掉，因此 callback
    /// 不能保存传入 slice。借用式 drain 避免了 `reader -> tmp -> plaintext Vec` 的
    /// staging copy。
    pub fn ingest_ciphertext<F>(
        &mut self,
        src: &[u8],
        dst_ciphertext: &mut Vec<u8>,
        mut on_plaintext: F,
    ) -> Result<(), TlsError>
    where
        F: FnMut(&[u8]),
    {
        let Some(mut stager) = self.record_stager.take() else {
            return self.ingest_ciphertext_direct(src, dst_ciphertext, &mut on_plaintext);
        };
        let result = stager.ingest(src, |record| {
            self.ingest_ciphertext_direct(record, dst_ciphertext, &mut on_plaintext)
        });
        self.record_stager = Some(stager);
        result
    }

    fn ingest_ciphertext_direct<F>(
        &mut self,
        mut src: &[u8],
        dst_ciphertext: &mut Vec<u8>,
        on_plaintext: &mut F,
    ) -> Result<(), TlsError>
    where
        F: FnMut(&[u8]),
    {
        let mut processed = false;
        while !src.is_empty() {
            if !self.conn.wants_read() {
                // rustls 的 deframer buffer 满了 —— 先 process_new_packets + drain
                // 让它腾位置；如果腾完还不想读，说明它确实暂时不需要更多字节
                // （处于 mid-record / post-close / 已经有完整 record 待处理等
                // 合法状态）。早期版本在此 return Err，会把合法状态当 fatal
                // 错误抛出关连接。正确做法：return Ok 让 caller 下一轮再喂。
                self.process_and_drain(dst_ciphertext, on_plaintext)?;
                if !self.conn.wants_read() {
                    return Ok(());
                }
            }

            let before = src.len();
            // `read_tls` reads from io::Read into rustls' internal buffer and
            // advances `src`. Process immediately after each socket chunk so a
            // pending deframer buffer never causes us to drop unread CQE bytes.
            let n = self.conn.read_tls(&mut src)?;
            if n == 0 || src.len() == before {
                break;
            }
            self.process_and_drain(dst_ciphertext, on_plaintext)?;
            processed = true;
        }
        if !processed {
            self.process_and_drain(dst_ciphertext, on_plaintext)?;
        }
        Ok(())
    }

    fn process_and_drain<F>(
        &mut self,
        dst_ciphertext: &mut Vec<u8>,
        on_plaintext: &mut F,
    ) -> Result<(), TlsError>
    where
        F: FnMut(&[u8]),
    {
        let io_state = self.conn.process_new_packets()?;
        // peer_has_closed 是一次性 latching 信号 —— 一旦 true 就不会再变 false。
        // 用 |= 保证即便 caller 在后续 process 中拿到 io_state 又被覆盖回 false（不会，
        // 但防御一下），我们这边永远保留 "见过" 状态。
        self.peer_closed_notify |= io_state.peer_has_closed();
        self.drain_plaintext(on_plaintext)?;
        self.drain_ciphertext(dst_ciphertext)?;
        Ok(())
    }

    fn drain_plaintext<F>(&mut self, on_plaintext: &mut F) -> Result<(), TlsError>
    where
        F: FnMut(&[u8]),
    {
        loop {
            let mut reader = self.conn.reader();
            match reader.fill_buf() {
                Ok([]) => break,
                Ok(chunk) => {
                    let n = chunk.len();
                    on_plaintext(chunk);
                    reader.consume(n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(TlsError::Io(e)),
            }
        }
        Ok(())
    }

    fn drain_ciphertext(&mut self, dst_ciphertext: &mut Vec<u8>) -> Result<(), TlsError> {
        while self.conn.wants_write() {
            let n = self.conn.write_tls(dst_ciphertext)?;
            if n == 0 {
                break;
            }
        }
        Ok(())
    }

    /// 把要发的明文交给 rustls 加密；密文 append 到 `dst_ciphertext`
    pub fn egress_plaintext(
        &mut self,
        src: &[u8],
        dst_ciphertext: &mut Vec<u8>,
    ) -> Result<(), TlsError> {
        if !src.is_empty() {
            std::io::Write::write_all(&mut self.conn.writer(), src)?;
        }
        self.drain_ciphertext(dst_ciphertext)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn app_data_record(payload: &[u8]) -> Vec<u8> {
        let len = u16::try_from(payload.len()).unwrap();
        let mut record = vec![0x17, 0x03, 0x03, (len >> 8) as u8, len as u8];
        record.extend_from_slice(payload);
        record
    }

    #[test]
    fn construct_with_valid_name() {
        // Doesn't connect — just builds the rustls state machine
        let r = TlsAdapter::new_client("www.example.com");
        assert!(r.is_ok());
    }

    #[test]
    fn handshake_initially_pending() {
        let t = TlsAdapter::new_client("www.example.com").unwrap();
        assert!(t.is_handshaking());
    }

    #[test]
    fn invalid_server_name_rejected() {
        // empty string is not a valid ServerName
        let r = TlsAdapter::new_client("");
        assert!(matches!(r, Err(TlsError::InvalidServerName(_))));
    }

    #[test]
    fn record_stager_passes_complete_records_directly() {
        let first = app_data_record(b"abc");
        let second = app_data_record(b"defgh");
        let wire = [first, second].concat();
        let mut got = Vec::new();
        let mut stager = TlsRecordStager::new();

        stager
            .ingest(&wire, |record| {
                got.push(record.to_vec());
                Ok(())
            })
            .unwrap();

        assert_eq!(
            got,
            vec![app_data_record(b"abc"), app_data_record(b"defgh")]
        );
        assert!(stager.pending.is_empty());
        assert_eq!(stager.stats.records, 2);
        assert_eq!(stager.stats.direct_records, 2);
        assert_eq!(stager.stats.staged_records, 0);
        assert_eq!(stager.stats.copied_bytes, 0);
    }

    #[test]
    fn record_stager_bounds_split_record_and_then_resumes_direct_path() {
        let first = app_data_record(b"abcdefgh");
        let second = app_data_record(b"xyz");
        let mut got = Vec::new();
        let mut stager = TlsRecordStager::new();

        stager
            .ingest(&first[..3], |record| {
                got.push(record.to_vec());
                Ok(())
            })
            .unwrap();
        assert!(got.is_empty());
        assert_eq!(stager.pending.len(), 3);

        let tail_and_second = [&first[3..], &second].concat();
        stager
            .ingest(&tail_and_second, |record| {
                got.push(record.to_vec());
                Ok(())
            })
            .unwrap();

        assert_eq!(got, vec![first.clone(), second]);
        assert!(stager.pending.is_empty());
        assert_eq!(stager.stats.records, 2);
        assert_eq!(stager.stats.direct_records, 1);
        assert_eq!(stager.stats.staged_records, 1);
        assert_eq!(stager.stats.copied_bytes, first.len() as u64);
        assert_eq!(stager.stats.max_pending_bytes, first.len() as u64);
    }

    #[test]
    fn record_stager_rejects_payload_at_rustls_limit() {
        let len = u16::try_from(TLS_RECORD_MAX_PAYLOAD_EXCLUSIVE).unwrap();
        let oversized = [0x17, 0x03, 0x03, (len >> 8) as u8, len as u8];
        let mut stager = TlsRecordStager::new();

        let err = stager.ingest(&oversized, |_| Ok(())).unwrap_err();

        assert!(matches!(
            err,
            TlsError::InvalidRecordHeader("payload exceeds rustls limit")
        ));
        assert!(stager.pending.is_empty());
    }
}
