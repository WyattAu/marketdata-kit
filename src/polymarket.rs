//! Polymarket CLOB market-channel WebSocket adapter.
//!
//! Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market`,
//! subscribes to one or more ERC-1155 asset ids, and forwards parsed
//! [`PolymarketWsEvent`]s (book snapshots, price changes, last trade
//! prices, tick-size changes) to a caller-supplied callback. Reconnects
//! with capped exponential backoff; no API key required.
//!
//! Numeric fields arrive as JSON strings on the wire; deserialization
//! accepts both strings and numbers, and `size: "0"` on a price change
//! means level deletion. Book snapshots carry `bids` ascending and
//! `asks` descending with the best level at the END of each array —
//! sort before treating either as an ordered book.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::error::MdkitError;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// One price level in a [`PolymarketBook`] snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolymarketLevel {
    /// Price of the level (wire value is a decimal string).
    #[serde(deserialize_with = "de_f64")]
    pub price: f64,
    /// Outstanding size at the level (wire value is a decimal string).
    #[serde(deserialize_with = "de_f64")]
    pub size: f64,
}

/// Full depth snapshot for one asset, published on subscribe and
/// periodically thereafter.
///
/// Note: on the wire `bids` arrive ASCENDING and `asks` DESCENDING, so
/// the best level of each side sits at the END of its array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolymarketBook {
    /// ERC-1155 token id this book belongs to.
    pub asset_id: String,
    /// Snapshot epoch milliseconds (wire value is a stringified integer).
    #[serde(deserialize_with = "de_i64")]
    pub timestamp: i64,
    /// Bid levels, ascending price (best last).
    pub bids: Vec<PolymarketLevel>,
    /// Ask levels, descending price (best last).
    pub asks: Vec<PolymarketLevel>,
}

/// Direction of a single price change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolymarketSide {
    /// Bid side.
    #[serde(rename = "BUY", alias = "buy")]
    Buy,
    /// Ask side.
    #[serde(rename = "SELL", alias = "sell")]
    Sell,
}

/// A single level change: absolute `price`/`size` pair. A `size` of
/// zero removes the level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolymarketPriceChange {
    /// ERC-1155 token id this change belongs to.
    pub asset_id: String,
    /// New level price (wire value is a decimal string).
    #[serde(deserialize_with = "de_f64")]
    pub price: f64,
    /// New level size; `0` deletes the level (wire value is a string).
    #[serde(deserialize_with = "de_f64")]
    pub size: f64,
    /// Side of the book this level belongs to.
    pub side: PolymarketSide,
}

/// Wire envelope for the `price_change` event: the payload carries a
/// LIST of changes under `price_changes`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolymarketPriceChanges {
    /// All level changes in the event, keyed per asset.
    pub price_changes: Vec<PolymarketPriceChange>,
}

/// Any market-channel event, discriminated by the wire `event_type`
/// field. Unknown event types parse to [`PolymarketWsEvent::Unknown`]
/// so the stream keeps flowing when Polymarket adds new ones.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum PolymarketWsEvent {
    /// Full depth snapshot (see [`PolymarketBook`] for ordering).
    Book(PolymarketBook),
    /// One or more level changes (see [`PolymarketPriceChanges`]).
    PriceChange(PolymarketPriceChanges),
    /// Last trade for an asset; `side`/`size` are absent for some assets.
    LastTradePrice {
        /// ERC-1155 token id the trade settled on.
        asset_id: String,
        /// Traded price (wire value is a decimal string).
        #[serde(deserialize_with = "de_f64")]
        price: f64,
        /// Taker side, when the exchange includes it.
        #[serde(default)]
        side: Option<PolymarketSide>,
        /// Traded size, when the exchange includes it.
        #[serde(default, deserialize_with = "de_opt_f64")]
        size: Option<f64>,
        /// Trade epoch milliseconds (wire value is a stringified integer).
        #[serde(deserialize_with = "de_i64")]
        timestamp: i64,
    },
    /// Minimum tick size for an asset.
    TickSize {
        /// ERC-1155 token id this tick size applies to.
        asset_id: String,
        /// Minimum price increment (wire value is a decimal string).
        #[serde(deserialize_with = "de_f64")]
        tick_size: f64,
        /// Event epoch milliseconds (wire value is a stringified integer).
        #[serde(deserialize_with = "de_i64")]
        timestamp: i64,
    },
    /// Recognized-but-unmodeled event type; carried so callers can log it.
    #[serde(other)]
    Unknown,
}

/// Deserializes an f64 from a wire decimal string (or a bare number).
fn de_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(text) => text.trim().parse::<f64>().map_err(serde::de::Error::custom),
        Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("number is not f64-representable")),
        other => Err(serde::de::Error::custom(format!(
            "expected decimal string, got {other}"
        ))),
    }
}

/// Deserializes an `Option<f64>` from a wire decimal string; absent or
/// null fields become `None`.
fn de_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => Ok(None),
        Some(value) => de_f64(value).map_err(serde::de::Error::custom).map(Some),
    }
}

/// Deserializes an i64 from a wire integer string (or a bare number).
fn de_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(text) => text.trim().parse::<i64>().map_err(serde::de::Error::custom),
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| serde::de::Error::custom("number is not i64-representable")),
        other => Err(serde::de::Error::custom(format!(
            "expected integer string, got {other}"
        ))),
    }
}

/// Subscribes to the market channel for `assets_ids` forever, invoking
/// `on_event` for every parsed event (batched frames are flattened into
/// individual dispatches). Reconnects with capped exponential backoff.
///
/// # Errors
/// Returns [`MdkitError`] when `assets_ids` is empty or the WebSocket
/// request cannot be built; transient transport failures are retried
/// in the reconnect loop instead.
pub async fn subscribe<F>(assets_ids: Vec<String>, on_event: F) -> Result<(), MdkitError>
where
    F: Fn(PolymarketWsEvent) + Send + 'static,
{
    if assets_ids.is_empty() {
        return Err(MdkitError::Malformed(
            "polymarket subscribe requires at least one asset id".to_string(),
        ));
    }
    let payload = serde_json::json!({ "assets_ids": assets_ids, "type": "market" });
    let mut backoff = Duration::from_secs(2);
    loop {
        match WS_URL.into_client_request() {
            Ok(request) => {
                tracing::info!(uri = %request.uri(), "polymarket ws dialing");
                match tokio_tungstenite::connect_async(request).await {
                    Ok((ws, _)) => {
                        tracing::info!("polymarket ws connected");
                        backoff = Duration::from_secs(2);
                        stream_session(ws, &payload, &on_event).await;
                        tracing::warn!("polymarket market stream closed; reconnecting");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "polymarket ws connect failed");
                    }
                }
            }
            Err(error) => {
                return Err(MdkitError::Ws(format!(
                    "polymarket ws request build failed: {error}"
                )));
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One connection lifetime: send the subscribe payload, pump messages,
/// answer pings. Returns when the stream closes or errors.
async fn stream_session<F, S>(
    ws: tokio_tungstenite::WebSocketStream<S>,
    payload: &Value,
    on_event: &F,
) where
    F: Fn(PolymarketWsEvent),
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut write, mut read) = ws.split();
    let subscribe_text = payload.to_string();
    if write
        .send(Message::Text(subscribe_text.into()))
        .await
        .is_err()
    {
        return;
    }
    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                let events = parse_events(&text);
                if events.is_empty() {
                    tracing::warn!(frame = %text, "polymarket frame parsed to no events");
                }
                for event in events {
                    on_event(event);
                }
            }
            Ok(Message::Ping(payload)) => {
                let _ = write.send(Message::Pong(payload)).await;
            }
            Ok(Message::Close(_)) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Parses one frame into events. The market channel can batch payloads
/// as a JSON array of event objects, so handle both shapes; anything
/// unparseable (or with an unknown `event_type`) contributes nothing.
fn parse_events(text: &str) -> Vec<PolymarketWsEvent> {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(rows)) => rows
            .into_iter()
            .filter_map(|row| serde_json::from_value(row).ok())
            .collect(),
        Ok(value) => serde_json::from_value(value).ok().into_iter().collect(),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
// Crate lints deny expect_used/panic for production code; tests assert on
// payloads and let-else refinement, where these are the honest calls.
#[allow(clippy::expect_used, clippy::panic, clippy::float_cmp)]
mod tests {
    use super::*;

    #[test]
    fn book_roundtrip_real_payload() {
        let raw = r#"{"event_type":"book","asset_id":"10530183335196013065967088325037845386457683763565929951923665853647643381204","timestamp":"1234567890123","bids":[{"price":"0.43","size":"120.0"}],"asks":[{"price":"0.44","size":"80.0"}]}"#;
        let event: PolymarketWsEvent = serde_json::from_str(raw).expect("test payload parses");
        let PolymarketWsEvent::Book(book) = event else {
            panic!("expected book variant");
        };
        assert_eq!(
            book.asset_id,
            "10530183335196013065967088325037845386457683763565929951923665853647643381204"
        );
        assert_eq!(book.timestamp, 1_234_567_890_123);
        assert_eq!(book.bids.first().map(|level| level.price), Some(0.43));
        assert_eq!(book.bids.first().map(|level| level.size), Some(120.0));
        assert_eq!(book.asks.first().map(|level| level.price), Some(0.44));
        assert_eq!(book.asks.first().map(|level| level.size), Some(80.0));

        let text =
            serde_json::to_string(&PolymarketWsEvent::Book(book.clone())).expect("serializes");
        let reparsed: PolymarketWsEvent = serde_json::from_str(&text).expect("re-parses");
        assert_eq!(reparsed, PolymarketWsEvent::Book(book));
        assert!(text.contains(r#""event_type":"book""#));
    }

    #[test]
    fn price_change_roundtrip_real_payload() {
        let raw = r#"{"event_type":"price_change","asset_id":"71321045679252212594626385532706912750332728571908433140412588821334748264166","market":"0x6d758d6df61df1d9fd859ae9184ad5e919cd0c9c","price_changes":[{"asset_id":"71321045679252212594626385532706912750332728571908433140412588821334748264166","price":"0.45","size":"0","side":"BUY"}],"timestamp":"1667851752","changes_count":1}"#;
        let event: PolymarketWsEvent = serde_json::from_str(raw).expect("test payload parses");
        let PolymarketWsEvent::PriceChange(changes) = event else {
            panic!("expected price_change variant");
        };
        assert_eq!(changes.price_changes.len(), 1);
        let change = changes.price_changes.first().cloned();
        let Some(change) = change else {
            panic!("expected one change");
        };
        assert_eq!(
            change.asset_id,
            "71321045679252212594626385532706912750332728571908433140412588821334748264166"
        );
        assert_eq!(change.price, 0.45);
        assert_eq!(change.size, 0.0); // "0" = level deletion
        assert_eq!(change.side, PolymarketSide::Buy);

        let text = serde_json::to_string(&PolymarketWsEvent::PriceChange(changes.clone()))
            .expect("serializes");
        let reparsed: PolymarketWsEvent = serde_json::from_str(&text).expect("re-parses");
        assert_eq!(reparsed, PolymarketWsEvent::PriceChange(changes));
        assert!(text.contains(r#""event_type":"price_change""#));
    }

    #[test]
    fn side_accepts_lowercase_alias() {
        let raw = r#"{"asset_id":"a","price":"0.5","size":"1","side":"buy"}"#;
        let change: PolymarketPriceChange = serde_json::from_str(raw).expect("parses");
        assert_eq!(change.side, PolymarketSide::Buy);
    }

    #[test]
    fn last_trade_price_roundtrip_real_payload() {
        let raw = r#"{"event_type":"last_trade_price","asset_id":"71321045679252212594626385532706912750332728571908433140412588821334748264166","price":"0.46","side":"SELL","size":"100","timestamp":"1667851752"}"#;
        let event: PolymarketWsEvent = serde_json::from_str(raw).expect("test payload parses");
        let PolymarketWsEvent::LastTradePrice {
            ref asset_id,
            price,
            side,
            size,
            timestamp,
        } = event
        else {
            panic!("expected last_trade_price variant");
        };
        assert_eq!(
            asset_id,
            "71321045679252212594626385532706912750332728571908433140412588821334748264166"
        );
        assert_eq!(price, 0.46);
        assert_eq!(side, Some(PolymarketSide::Sell));
        assert_eq!(size, Some(100.0));
        assert_eq!(timestamp, 1_667_851_752);

        let text = serde_json::to_string(&event).expect("serializes");
        let reparsed: PolymarketWsEvent = serde_json::from_str(&text).expect("re-parses");
        assert_eq!(reparsed, event);
        assert!(text.contains(r#""event_type":"last_trade_price""#));
    }

    #[test]
    fn last_trade_price_without_side_size() {
        let raw = r#"{"event_type":"last_trade_price","asset_id":"a","price":"0.31","timestamp":"1667851752"}"#;
        let event: PolymarketWsEvent = serde_json::from_str(raw).expect("parses");
        let PolymarketWsEvent::LastTradePrice { side, size, .. } = event else {
            panic!("expected last_trade_price variant");
        };
        assert_eq!(side, None);
        assert_eq!(size, None);
    }

    #[test]
    fn tick_size_roundtrip_real_payload() {
        let raw = r#"{"event_type":"tick_size","asset_id":"71321045679252212594626385532706912750332728571908433140412588821334748264166","tick_size":"0.01","timestamp":"1667851752"}"#;
        let event: PolymarketWsEvent = serde_json::from_str(raw).expect("test payload parses");
        let PolymarketWsEvent::TickSize {
            tick_size,
            timestamp,
            ..
        } = event
        else {
            panic!("expected tick_size variant");
        };
        assert_eq!(tick_size, 0.01);
        assert_eq!(timestamp, 1_667_851_752);

        let text = serde_json::to_string(&event).expect("serializes");
        let reparsed: PolymarketWsEvent = serde_json::from_str(&text).expect("re-parses");
        assert_eq!(reparsed, event);
        assert!(text.contains(r#""event_type":"tick_size""#));
    }

    #[test]
    fn batched_array_frame_flattens() {
        let raw = r#"[{"event_type":"tick_size","asset_id":"a","tick_size":"0.01","timestamp":"1"},{"event_type":"book","asset_id":"b","timestamp":"2","bids":[],"asks":[]}]"#;
        let events = parse_events(raw);
        assert_eq!(events.len(), 2);
        assert_eq!(
            events.first(),
            Some(&PolymarketWsEvent::TickSize {
                asset_id: "a".to_string(),
                tick_size: 0.01,
                timestamp: 1,
            })
        );
    }

    #[test]
    fn unknown_event_type_is_unknown_variant() {
        let raw = r#"{"event_type":"brand_new_thing","asset_id":"a"}"#;
        let event: PolymarketWsEvent = serde_json::from_str(raw).expect("parses");
        assert_eq!(event, PolymarketWsEvent::Unknown);
    }
}
