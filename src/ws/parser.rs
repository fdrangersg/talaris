//! 流式 frame parser —— 字节进事件出
//!
//! 只缓存 ≤14 字节的 header partial buf；payload 不缓存，以 `&[u8]` 切片转手
//! 给 caller，由上层 [`super::client::WsClient`] 决定要不要 append 到 message buf。
//!
//! 这样 parser 自身始终 O(1) 状态，无视 payload 大小（最大可到 GiB 级）。

#![allow(clippy::cast_possible_truncation, clippy::cast_lossless)]

use super::frame::{FrameError, FrameHeader, MAX_HEADER_LEN, parse_header};

/// 解析事件
#[derive(Clone, Copy, Debug)]
pub enum FrameEvent<'a> {
    /// 帧头到齐
    FrameStart(FrameHeader),
    /// 一段 payload（流式，可能多次出现直到 FrameEnd）
    PayloadChunk(&'a [u8]),
    /// 当前帧 payload 收完
    FrameEnd,
}

/// feed_one 的结果
#[derive(Debug)]
pub enum FeedOutcome<'a> {
    /// 当前缓冲不够推进；`consumed` 是被消化到 parser 内部状态的字节数
    NeedMore { consumed: usize },
    /// 产生一个事件
    Event {
        consumed: usize,
        event: FrameEvent<'a>,
    },
}

#[derive(Debug)]
enum ParserState {
    Idle,
    HeaderPartial {
        partial: [u8; MAX_HEADER_LEN],
        filled: u8,
    },
    Payload {
        remaining: u64,
    },
}

/// 流式 frame parser
#[derive(Debug)]
pub struct FrameParser {
    state: ParserState,
}

impl Default for FrameParser {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameParser {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: ParserState::Idle,
        }
    }

    #[inline]
    #[must_use]
    pub const fn is_idle(&self) -> bool {
        matches!(self.state, ParserState::Idle)
    }

    /// 推一段字节给 parser，最多吐一个事件。
    ///
    /// 调用者循环直到 `NeedMore` 才回到 IO 层取更多字节。
    pub fn feed_one<'a>(&mut self, mut bytes: &'a [u8]) -> Result<FeedOutcome<'a>, FrameError> {
        let mut total_consumed: usize = 0;

        loop {
            match &mut self.state {
                ParserState::Idle => {
                    self.state = ParserState::HeaderPartial {
                        partial: [0; MAX_HEADER_LEN],
                        filled: 0,
                    };
                    // fall through to HeaderPartial branch next iter
                }
                ParserState::HeaderPartial { partial, filled } => {
                    let min_needed = if *filled < 2 {
                        2
                    } else {
                        header_size_from_byte1(partial[1])
                    };

                    if (*filled as usize) >= min_needed {
                        // Have full header — parse
                        let snapshot = *partial;
                        let len = *filled as usize;
                        // Unreachable：filled == min_needed == 真实 header size，parse_header
                        // 一定 yield `Some(_)`。原先用 `Err(RsvBitsSet)` 占位会把内部 bug
                        // 伪装成协议错——故障排查时严重误导，改成 unreachable!() 直接暴露。
                        let (header, _hsize) =
                            parse_header(&snapshot[..len])?.unwrap_or_else(|| {
                                unreachable!("filled >= min_needed: parse_header must yield Some")
                            });

                        // Server→client frames MUST NOT be masked (RFC §5.1)
                        if header.mask.is_some() {
                            return Err(FrameError::ServerSentMaskedFrame);
                        }

                        self.state = ParserState::Payload {
                            remaining: header.payload_len,
                        };
                        return Ok(FeedOutcome::Event {
                            consumed: total_consumed,
                            event: FrameEvent::FrameStart(header),
                        });
                    }

                    // Need more bytes for header
                    let want = min_needed - *filled as usize;
                    let take = want.min(bytes.len());
                    if take == 0 {
                        return Ok(FeedOutcome::NeedMore {
                            consumed: total_consumed,
                        });
                    }
                    let start = *filled as usize;
                    partial[start..start + take].copy_from_slice(&bytes[..take]);
                    *filled += take as u8;
                    bytes = &bytes[take..];
                    total_consumed += take;
                    // loop: may need another iter if we hit the 2-byte peek boundary
                }
                ParserState::Payload { remaining } => {
                    if *remaining == 0 {
                        self.state = ParserState::Idle;
                        return Ok(FeedOutcome::Event {
                            consumed: total_consumed,
                            event: FrameEvent::FrameEnd,
                        });
                    }
                    if bytes.is_empty() {
                        return Ok(FeedOutcome::NeedMore {
                            consumed: total_consumed,
                        });
                    }
                    let take = bytes.len().min(*remaining as usize);
                    *remaining -= take as u64;
                    let chunk = &bytes[..take];
                    return Ok(FeedOutcome::Event {
                        consumed: total_consumed + take,
                        event: FrameEvent::PayloadChunk(chunk),
                    });
                }
            }
        }
    }
}

/// 已知 byte1，推算完整 header 字节数（含 ext len + mask key）
#[inline]
fn header_size_from_byte1(b1: u8) -> usize {
    let masked = (b1 & 0x80) != 0;
    let len7 = b1 & 0x7F;
    // len7 = b1 & 0x7F → 0..=127；下面三条 arm 已穷尽。
    let len_field = match len7 {
        0..=125 => 0,
        126 => 2,
        127 => 8,
        _ => unreachable!("len7 = b1 & 0x7F is 0..=127"),
    };
    let mask_field = if masked { 4 } else { 0 };
    2 + len_field + mask_field
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::ws::frame::OpCode;

    /// 把整个 input 一次 feed 完，收集所有事件
    fn drain_all<'a>(p: &mut FrameParser, mut bytes: &'a [u8]) -> Vec<DrainEvent> {
        let mut out = Vec::new();
        loop {
            match p.feed_one(bytes).unwrap() {
                FeedOutcome::NeedMore { .. } => {
                    out.push(DrainEvent::NeedMore);
                    return out;
                }
                FeedOutcome::Event { consumed, event } => {
                    out.push(match event {
                        FrameEvent::FrameStart(h) => DrainEvent::Start(h),
                        FrameEvent::PayloadChunk(c) => DrainEvent::Chunk(c.to_vec()),
                        FrameEvent::FrameEnd => DrainEvent::End,
                    });
                    bytes = &bytes[consumed..];
                }
            }
        }
    }

    #[derive(Debug)]
    enum DrainEvent {
        Start(FrameHeader),
        Chunk(Vec<u8>),
        End,
        NeedMore,
    }

    impl DrainEvent {
        fn is_start(&self) -> bool {
            matches!(self, Self::Start(_))
        }
        fn is_end(&self) -> bool {
            matches!(self, Self::End)
        }
    }

    #[test]
    fn complete_small_text_one_call() {
        // Server→client text "Hello", FIN=1, unmasked
        let frame = b"\x81\x05Hello";
        let mut p = FrameParser::new();
        let events = drain_all(&mut p, frame);
        assert!(events[0].is_start());
        match &events[1] {
            DrainEvent::Chunk(c) => assert_eq!(c, b"Hello"),
            other => panic!("expected Chunk, got {other:?}"),
        }
        assert!(events[2].is_end());
        assert!(matches!(events[3], DrainEvent::NeedMore));
    }

    #[test]
    fn byte_by_byte_drip_feed() {
        let frame = b"\x81\x05Hello";
        let mut p = FrameParser::new();
        let mut all_events = Vec::new();
        for chunk in frame.chunks(1) {
            let mut remaining = chunk;
            loop {
                match p.feed_one(remaining).unwrap() {
                    FeedOutcome::NeedMore { consumed } => {
                        remaining = &remaining[consumed..];
                        if remaining.is_empty() {
                            break;
                        }
                    }
                    FeedOutcome::Event { consumed, event } => {
                        all_events.push(format!("{event:?}"));
                        remaining = &remaining[consumed..];
                    }
                }
            }
        }
        // Should see: FrameStart, PayloadChunk*N, FrameEnd
        assert!(all_events.iter().any(|s| s.starts_with("FrameStart")));
        assert!(all_events.iter().any(|s| s == "FrameEnd"));
        let chunk_count = all_events
            .iter()
            .filter(|s| s.starts_with("PayloadChunk"))
            .count();
        // Drip-fed bytes produce many tiny chunks
        assert_eq!(chunk_count, 5);
    }

    #[test]
    fn extended_16bit_len_frame() {
        // FIN=1, opcode=Binary, len7=126, ext_len=300
        let mut frame: Vec<u8> = vec![0x82, 0x7E, 0x01, 0x2C];
        frame.extend(std::iter::repeat_n(0xAB, 300));
        let mut p = FrameParser::new();
        let events = drain_all(&mut p, &frame);
        match &events[0] {
            DrainEvent::Start(h) => {
                assert_eq!(h.opcode, OpCode::Binary);
                assert_eq!(h.payload_len, 300);
            }
            other => panic!("{other:?}"),
        }
        let total_payload: usize = events
            .iter()
            .filter_map(|e| match e {
                DrainEvent::Chunk(c) => Some(c.len()),
                _ => None,
            })
            .sum();
        assert_eq!(total_payload, 300);
        assert!(events.iter().any(|e| e.is_end()));
    }

    #[test]
    fn split_header_across_calls() {
        // Single frame, fed in 3 chunks: header byte 0 alone, header byte 1 alone, payload
        let mut p = FrameParser::new();
        let r1 = p.feed_one(&[0x81]).unwrap();
        assert!(matches!(r1, FeedOutcome::NeedMore { consumed: 1 }));
        let r2 = p.feed_one(&[0x03]).unwrap();
        // After getting both header bytes, parse → FrameStart
        match r2 {
            FeedOutcome::Event {
                consumed: 1,
                event: FrameEvent::FrameStart(h),
            } => {
                assert_eq!(h.payload_len, 3);
            }
            other => panic!("expected FrameStart, got {other:?}"),
        }
        let r3 = p.feed_one(b"abc").unwrap();
        match r3 {
            FeedOutcome::Event {
                consumed: 3,
                event: FrameEvent::PayloadChunk(c),
            } => assert_eq!(c, b"abc"),
            other => panic!("{other:?}"),
        }
        let r4 = p.feed_one(&[]).unwrap();
        assert!(matches!(
            r4,
            FeedOutcome::Event {
                consumed: 0,
                event: FrameEvent::FrameEnd
            }
        ));
    }

    #[test]
    fn empty_payload_frame() {
        // FIN=1, Ping with 0-length payload
        let frame = b"\x89\x00";
        let mut p = FrameParser::new();
        let events = drain_all(&mut p, frame);
        assert!(events[0].is_start());
        assert!(events[1].is_end()); // no PayloadChunk for empty payload
    }

    #[test]
    fn rsv_bits_rejected() {
        let frame = b"\xC1\x00"; // RSV1=1
        let mut p = FrameParser::new();
        let err = p.feed_one(frame).unwrap_err();
        assert_eq!(err, FrameError::RsvBitsSet);
    }

    #[test]
    fn masked_server_frame_rejected() {
        // Server should never mask. We're a client, so seeing mask=1 is protocol error.
        let frame = b"\x81\x85\x12\x34\x56\x78Hello"; // FIN=1, Text, masked, len=5
        let mut p = FrameParser::new();
        let err = p.feed_one(frame).unwrap_err();
        assert_eq!(err, FrameError::ServerSentMaskedFrame);
    }

    #[test]
    fn two_back_to_back_frames() {
        // Two complete text frames concatenated
        let mut data: Vec<u8> = vec![];
        data.extend_from_slice(b"\x81\x02hi");
        data.extend_from_slice(b"\x81\x03foo");
        let mut p = FrameParser::new();
        let events = drain_all(&mut p, &data);
        let start_count = events.iter().filter(|e| e.is_start()).count();
        let end_count = events.iter().filter(|e| e.is_end()).count();
        assert_eq!(start_count, 2);
        assert_eq!(end_count, 2);
    }
}
