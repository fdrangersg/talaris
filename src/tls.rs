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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsCryptoProvider {
    AwsLc,
    Ring,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsCipherPreference {
    #[default]
    ProviderDefault,
    Aes128GcmFirst,
    Aes256GcmFirst,
    Chacha20First,
}

impl Default for TlsCryptoProvider {
    fn default() -> Self {
        Self::Ring
    }
}

impl std::fmt::Display for TlsCryptoProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AwsLc => f.write_str("aws-lc"),
            Self::Ring => f.write_str("ring"),
        }
    }
}

impl std::str::FromStr for TlsCryptoProvider {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "aws-lc" | "aws_lc" | "aws-lc-rs" | "aws_lc_rs" => Ok(Self::AwsLc),
            "ring" => Ok(Self::Ring),
            _ => Err(format!(
                "invalid tls provider {s:?}; expected aws-lc or ring"
            )),
        }
    }
}

impl std::fmt::Display for TlsCipherPreference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProviderDefault => f.write_str("default"),
            Self::Aes128GcmFirst => f.write_str("aes128"),
            Self::Aes256GcmFirst => f.write_str("aes256"),
            Self::Chacha20First => f.write_str("chacha"),
        }
    }
}

impl std::str::FromStr for TlsCipherPreference {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "default" => Ok(Self::ProviderDefault),
            "aes128" | "aes-128" | "aes128-gcm" | "aes-128-gcm" => Ok(Self::Aes128GcmFirst),
            "aes256" | "aes-256" | "aes256-gcm" | "aes-256-gcm" => Ok(Self::Aes256GcmFirst),
            "chacha" | "chacha20" | "chacha20-poly1305" => Ok(Self::Chacha20First),
            _ => Err(format!(
                "invalid tls cipher preference {s:?}; expected default, aes128, aes256, or chacha"
            )),
        }
    }
}

/// rustls client 包装
pub struct TlsAdapter {
    conn: rustls::ClientConnection,
    /// `IoState::peer_has_closed` 的最新观察。每次 `process_new_packets` 后刷新。
    /// rustls 0.23 不在 `ClientConnection` 上直接暴露这个 flag —— 它只在
    /// `process_new_packets` 返回的 `IoState` 上。我们 cache 一下。
    peer_closed_notify: bool,
}

impl std::fmt::Debug for TlsAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsAdapter")
            .field("is_handshaking", &self.conn.is_handshaking())
            .finish()
    }
}

impl TlsAdapter {
    /// 构造 client。`server_name` 通常是连接的主机名（用于 SNI + 证书校验）。
    pub fn new_client(server_name: &str) -> Result<Self, TlsError> {
        Self::new_client_with_provider(server_name, TlsCryptoProvider::default())
    }

    /// 使用指定 crypto provider 构造 client。默认生产路径使用
    /// [`TlsCryptoProvider::Ring`]，这是当前 Linux 行情订阅基准上延迟更低的
    /// provider；`AwsLc` 保留给机器相关 A/B、FIPS 或 PQ 需求。
    pub fn new_client_with_provider(
        server_name: &str,
        provider: TlsCryptoProvider,
    ) -> Result<Self, TlsError> {
        Self::new_client_with_config(
            server_name,
            Arc::new(client_config(
                provider,
                TlsCipherPreference::ProviderDefault,
            )?),
        )
    }

    /// 构造 rustls client config。公开给 benchmark 或上层组合代码，保证
    /// tungstenite 对照和 talaris 使用完全相同的 provider/root/alpn 配置。
    pub fn client_config(provider: TlsCryptoProvider) -> Result<rustls::ClientConfig, TlsError> {
        client_config(provider, TlsCipherPreference::ProviderDefault)
    }

    pub fn client_config_with_cipher_preference(
        provider: TlsCryptoProvider,
        preference: TlsCipherPreference,
    ) -> Result<rustls::ClientConfig, TlsError> {
        client_config(provider, preference)
    }

    #[must_use]
    pub fn negotiated_cipher_suite(&self) -> Option<rustls::CipherSuite> {
        self.conn
            .negotiated_cipher_suite()
            .map(|suite| suite.suite())
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
        })
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
        mut src: &[u8],
        dst_ciphertext: &mut Vec<u8>,
        mut on_plaintext: F,
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
                self.process_and_drain(dst_ciphertext, &mut on_plaintext)?;
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
            self.process_and_drain(dst_ciphertext, &mut on_plaintext)?;
            processed = true;
        }
        if !processed {
            self.process_and_drain(dst_ciphertext, &mut on_plaintext)?;
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
        let mut reader = self.conn.reader();
        loop {
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

fn client_config(
    provider: TlsCryptoProvider,
    preference: TlsCipherPreference,
) -> Result<rustls::ClientConfig, TlsError> {
    let root_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let mut provider = match provider {
        TlsCryptoProvider::AwsLc => rustls::crypto::aws_lc_rs::default_provider(),
        TlsCryptoProvider::Ring => rustls::crypto::ring::default_provider(),
    };
    apply_cipher_preference(&mut provider, preference);
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(root_store)
        .with_no_client_auth();
    // 显式只接受 HTTP/1.1，避免 server 协商 HTTP/2 后 WS upgrade 不通。
    // 仅"通告"还不够 —— handshake 完后必须再用 [`TlsAdapter::verify_alpn`]
    // 校验 server 真的回了 http/1.1（或不回）。
    config.alpn_protocols = vec![REQUIRED_ALPN.to_vec()];
    Ok(config)
}

fn apply_cipher_preference(
    provider: &mut rustls::crypto::CryptoProvider,
    preference: TlsCipherPreference,
) {
    let preferred = match preference {
        TlsCipherPreference::ProviderDefault => return,
        TlsCipherPreference::Aes128GcmFirst => &[
            rustls::CipherSuite::TLS13_AES_128_GCM_SHA256,
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
            rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
        ][..],
        TlsCipherPreference::Aes256GcmFirst => &[
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
            rustls::CipherSuite::TLS13_AES_128_GCM_SHA256,
            rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
        ][..],
        TlsCipherPreference::Chacha20First => &[
            rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
            rustls::CipherSuite::TLS13_AES_128_GCM_SHA256,
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
        ][..],
    };

    let mut reordered = Vec::with_capacity(provider.cipher_suites.len());
    for target in preferred {
        if let Some(suite) = provider
            .cipher_suites
            .iter()
            .copied()
            .find(|suite| suite.suite() == *target)
        {
            reordered.push(suite);
        }
    }
    for suite in provider.cipher_suites.iter().copied() {
        if reordered
            .iter()
            .all(|existing| existing.suite() != suite.suite())
        {
            reordered.push(suite);
        }
    }
    provider.cipher_suites = reordered;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
}
