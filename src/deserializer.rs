/// This file is modified from Twilight to also include the position of each
///
/// ISC License (ISC)
///
/// Copyright (c) 2019 (c) The Twilight Contributors
///
/// Permission to use, copy, modify, and/or distribute this software for any purpose
/// with or without fee is hereby granted, provided that the above copyright notice
/// and this permission notice appear in all copies.
///
/// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
/// REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
/// AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
/// INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
/// OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
/// TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE
/// OF THIS SOFTWARE.
use std::{ops::Range, str::FromStr};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayEvent<'a> {
    event_type: Option<EventTypeInfo<'a>>,
    op: OpInfo,
    sequence: Option<SequenceInfo>,
    guild_id: Option<u64>,
    channel_id: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpInfo(pub u8, pub Range<usize>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventTypeInfo<'a>(pub &'a str, pub Range<usize>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequenceInfo(pub u64, pub Range<usize>);

impl<'a> GatewayEvent<'a> {
    /// Create a gateway event deserializer with some information found by
    /// scanning the JSON payload to deserialise.
    ///
    /// This will scan the payload for the opcode and, optionally, event type if
    /// provided. The opcode key ("op"), must be in the payload while the event
    /// type key ("t") is optional and only required for event ops.
    pub fn from_json(input: &'a str) -> Option<Self> {
        let op = Self::find_opcode(input)?;
        let event_type = Self::find_event_type(input);
        let sequence = Self::find_sequence(input);
        let guild_id = Self::find_guild_id(input, event_type.as_ref());
        let channel_id = Self::find_channel_id(input, event_type.as_ref());

        Some(Self {
            event_type,
            op,
            sequence,
            guild_id,
            channel_id,
        })
    }

    /// Return the opcode of the payload.
    pub const fn op(&self) -> u8 {
        self.op.0
    }

    /// Return the guild_id if present in the payload.
    #[allow(dead_code)]
    pub const fn guild_id(&self) -> Option<u64> {
        self.guild_id
    }

    /// Consume the deserializer, returning its opcode and event type
    /// components.
    pub const fn into_parts(
        self,
    ) -> (
        OpInfo,
        Option<SequenceInfo>,
        Option<EventTypeInfo<'a>>,
        Option<u64>,
        Option<u64>,
    ) {
        (
            self.op,
            self.sequence,
            self.event_type,
            self.guild_id,
            self.channel_id,
        )
    }

    fn find_event_type(input: &'a str) -> Option<EventTypeInfo<'a>> {
        // We're going to search for the event type key from the start. Discord
        // always puts it at the front before the D key from some testing of
        // several hundred payloads.
        //
        // If we find it, add 4, since that's the length of what we're searching
        // for.
        let from = input.find(r#""t":"#)? + 4;

        // Now let's find where the value starts, which may be a string or null.
        // Or maybe something else. If it's anything but a string, then there's
        // no event type.
        let start = input.get(from..)?.find(|c: char| !c.is_whitespace())? + from + 1;

        // Check if the character just before the cursor is '"'.
        if input.as_bytes().get(start - 1).copied()? != b'"' {
            return None;
        }

        let to = input.get(start..)?.find('"')?;
        let range = start..start + to;

        input
            .get(range.clone())
            .map(|event_type| EventTypeInfo(event_type, range))
    }

    fn find_opcode(input: &'a str) -> Option<OpInfo> {
        Self::find_integer(input, r#""op":"#).map(|(op, pos)| OpInfo(op, pos))
    }

    fn find_sequence(input: &'a str) -> Option<SequenceInfo> {
        Self::find_integer(input, r#""s":"#).map(|(seq, pos)| SequenceInfo(seq, pos))
    }

    fn find_integer<T: FromStr>(input: &'a str, key: &str) -> Option<(T, Range<usize>)> {
        // Find the op key's position and then search for where the first
        // character that's not base 10 is. This'll give us the bytes with the
        // op which can be parsed.
        //
        // Add 5 at the end since that's the length of what we're finding.
        let from = input.find(key)? + key.len();

        // Look for the first thing that isn't a base 10 digit or whitespace,
        // i.e. a comma (denoting another JSON field), curly brace (end of the
        // object), etc. This'll give us the op number, maybe with a little
        // whitespace.
        let to = input.get(from..)?.find(&[',', '}'] as &[_])?;
        let range = from..from + to;
        let clean = input.get(range.clone())?;

        T::from_str(clean).ok().map(|int| (int, range))
    }

    fn find_data_field_u64(input: &'a str, field: &str) -> Option<u64> {
        let data_start = input.find("\"d\":")?;
        let data_slice = input.get(data_start..)?;

        let key = format!(r#""{field}""#);
        let key_start = data_slice.find(&key)?;
        let key_end = key_start + key.len();
        let after_key = data_slice.get(key_end..)?;
        let colon_pos = after_key.find(':')?;
        let after_colon = after_key.get(colon_pos + 1..)?;
        let ws_offset = after_colon.find(|c: char| !c.is_whitespace())?;
        let value = after_colon.get(ws_offset..)?;

        let first = value.as_bytes().first().copied()?;

        if first == b'"' {
            let value_end = value.get(1..)?.find('"')? + 1;
            return value.get(1..value_end)?.parse().ok();
        }

        if first == b'n' {
            return None;
        }

        let value_end = value
            .find(|c: char| c == ',' || c == '}' || c.is_whitespace())
            .unwrap_or(value.len());
        value.get(..value_end)?.parse().ok()
    }

    /// Find the guild snowflake used for event routing.
    ///
    /// Most dispatch events include `d.guild_id`. Guild lifecycle events use
    /// `d.id` as the guild identifier.
    fn find_guild_id(input: &'a str, event_type: Option<&EventTypeInfo<'_>>) -> Option<u64> {
        if let Some(guild_id) = Self::find_data_field_u64(input, "guild_id") {
            return Some(guild_id);
        }

        let Some(event_type_info) = event_type else {
            return None;
        };

        let event_name = event_type_info.0;

        if matches!(event_name, "GUILD_CREATE" | "GUILD_DELETE" | "GUILD_UPDATE") {
            return Self::find_data_field_u64(input, "id");
        }

        None
    }

    fn find_channel_id(input: &'a str, event_type: Option<&EventTypeInfo<'_>>) -> Option<u64> {
        if let Some(channel_id) = Self::find_data_field_u64(input, "channel_id") {
            return Some(channel_id);
        }

        let Some(event_type_info) = event_type else {
            return None;
        };

        if matches!(
            event_type_info.0,
            "CHANNEL_CREATE"
                | "CHANNEL_UPDATE"
                | "CHANNEL_DELETE"
                | "THREAD_CREATE"
                | "THREAD_UPDATE"
                | "THREAD_DELETE"
        ) {
            return Self::find_data_field_u64(input, "id");
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::GatewayEvent;

    #[test]
    fn finds_channel_id_on_message_create() {
        let payload = r#"{"t":"MESSAGE_CREATE","s":1,"op":0,"d":{"id":"9","channel_id":"1545800148565758123","guild_id":"1422625037164351591","content":"hi"}}"#;
        let event = GatewayEvent::from_json(payload).expect("event");
        let (_op, _seq, event_type, guild_id, channel_id) = event.into_parts();
        assert_eq!(event_type.map(|info| info.0), Some("MESSAGE_CREATE"));
        assert_eq!(guild_id, Some(1_422_625_037_164_351_591));
        assert_eq!(channel_id, Some(1_545_800_148_565_758_123));
    }
}
