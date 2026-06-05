//! RFC 6455 §7 Closing the Connection —— close 状态码 + payload codec
//!
//! Close frame payload 格式（RFC §5.5.1）：
//! - 0 字节：无 code、无 reason（被解释为 1005 "No Status Rcvd"）
//! - 2 字节：u16 BE close code
//! - 2 + N 字节：code + UTF-8 reason（reason ≤ 123 字节，因为 control frame
//!   payload 上限 125）
//!
//! Close code 分类（RFC §7.4）：
//! - 1000-1011：协议层（部分 endpoint 不可主动发）
//! - 1015：TLS 失败（内部）
//! - 3000-3999：framework / library 保留
//! - 4000-4999：application 自定义

use thiserror::Error;

/// 标准 close code（RFC §7.4.1）
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum CloseCode {
    Normal = 1000,
    GoingAway = 1001,
    ProtocolError = 1002,
    UnsupportedData = 1003,
    InvalidPayload = 1007,
    PolicyViolation = 1008,
    MessageTooBig = 1009,
    MandatoryExt = 1010,
    InternalError = 1011,
}

impl CloseCode {
    #[inline]
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CloseError {
    #[error("close payload has 1 byte (must be 0 or ≥2)")]
    OneByte,
    #[error("close payload reason is not valid UTF-8")]
    InvalidUtf8,
    #[error("close code {0} is reserved or invalid")]
    InvalidCode(u16),
}

/// 检查 close code 是否 endpoint 可主动发出（RFC §7.4.2）
#[must_use]
pub const fn is_valid_endpoint_sent(code: u16) -> bool {
    matches!(
        code,
        1000..=1003 | 1007..=1011 | 3000..=4999
    )
}

/// 检查 close code 是否可在 close frame payload 中合法出现（包括对端发来的）
#[must_use]
pub const fn is_valid_received(code: u16) -> bool {
    // 1004/1005/1006/1012-1014/1015/1016-2999 都是 reserved，不能出现在 wire 上
    matches!(
        code,
        1000..=1003 | 1007..=1011 | 3000..=4999
    )
}

/// 解析 close payload，返回 `(code, reason)`。
///
/// payload 为空时返回 `Ok(None)`，代表对端没给 code（按 RFC 视为 1005）。
pub fn parse_close_payload(payload: &[u8]) -> Result<Option<(u16, &str)>, CloseError> {
    match payload.len() {
        0 => Ok(None),
        1 => Err(CloseError::OneByte),
        _ => {
            let code = u16::from_be_bytes([payload[0], payload[1]]);
            if !is_valid_received(code) {
                return Err(CloseError::InvalidCode(code));
            }
            let reason_bytes = &payload[2..];
            let reason = std::str::from_utf8(reason_bytes).map_err(|_| CloseError::InvalidUtf8)?;
            Ok(Some((code, reason)))
        }
    }
}

/// 编码 close payload 到 `dst`，返回写入字节数。
///
/// `reason` 必须是合法 UTF-8 且 ≤ 123 字节。`dst` 必须 ≥ 125 字节。
/// caller 负责检查 code 是否 endpoint-sendable（用 [`is_valid_endpoint_sent`]）。
pub fn encode_close_payload(dst: &mut [u8], code: u16, reason: &str) -> usize {
    debug_assert!(dst.len() >= 2 + reason.len());
    debug_assert!(reason.len() <= 123);
    debug_assert!(std::str::from_utf8(reason.as_bytes()).is_ok());

    let code_bytes = code.to_be_bytes();
    dst[0] = code_bytes[0];
    dst[1] = code_bytes[1];
    let reason_len = reason.len();
    dst[2..2 + reason_len].copy_from_slice(reason.as_bytes());
    2 + reason_len
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn empty_payload_means_no_code() {
        assert_eq!(parse_close_payload(&[]).unwrap(), None);
    }

    #[test]
    fn one_byte_is_protocol_error() {
        assert_eq!(
            parse_close_payload(&[0x03]).unwrap_err(),
            CloseError::OneByte
        );
    }

    #[test]
    fn code_only_normal() {
        let payload = [0x03, 0xE8]; // 1000
        let (code, reason) = parse_close_payload(&payload).unwrap().unwrap();
        assert_eq!(code, 1000);
        assert_eq!(reason, "");
    }

    #[test]
    fn code_plus_reason() {
        let mut payload: Vec<u8> = vec![0x03, 0xE9]; // 1001 going away
        payload.extend_from_slice(b"bye");
        let (code, reason) = parse_close_payload(&payload).unwrap().unwrap();
        assert_eq!(code, 1001);
        assert_eq!(reason, "bye");
    }

    #[test]
    fn invalid_code_rejected() {
        // 1006 abnormal closure (internal-only)
        let payload = [0x03, 0xEE];
        let err = parse_close_payload(&payload).unwrap_err();
        assert!(matches!(err, CloseError::InvalidCode(1006)));
        // 2999 reserved
        let payload = [0x0B, 0xB7];
        let err = parse_close_payload(&payload).unwrap_err();
        assert!(matches!(err, CloseError::InvalidCode(2999)));
    }

    #[test]
    fn application_code_4xxx_ok() {
        let payload = [0x0F, 0xA0]; // 4000
        let (code, _) = parse_close_payload(&payload).unwrap().unwrap();
        assert_eq!(code, 4000);
    }

    #[test]
    fn invalid_utf8_reason_rejected() {
        let payload = [0x03, 0xE8, 0xFF, 0xFE];
        let err = parse_close_payload(&payload).unwrap_err();
        assert_eq!(err, CloseError::InvalidUtf8);
    }

    #[test]
    fn encode_then_parse_roundtrip() {
        let mut buf = [0_u8; 125];
        let n = encode_close_payload(&mut buf, 1000, "bye");
        assert_eq!(n, 5);
        let (code, reason) = parse_close_payload(&buf[..n]).unwrap().unwrap();
        assert_eq!(code, 1000);
        assert_eq!(reason, "bye");
    }

    #[test]
    fn endpoint_sendable_validation() {
        assert!(is_valid_endpoint_sent(1000));
        assert!(is_valid_endpoint_sent(1011));
        assert!(is_valid_endpoint_sent(4000));
        assert!(!is_valid_endpoint_sent(1004));
        assert!(!is_valid_endpoint_sent(1005));
        assert!(!is_valid_endpoint_sent(1006));
        assert!(!is_valid_endpoint_sent(1015));
        assert!(!is_valid_endpoint_sent(2999));
        assert!(!is_valid_endpoint_sent(5000));
    }
}
