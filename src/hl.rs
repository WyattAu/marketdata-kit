//! Hyperliquid public REST adapter.
//!
//! Everything is a `POST /info` against `https://api.hyperliquid.xyz`
//! with a JSON body describing the query. No API key required.
//!
//! Numeric fields arrive as JSON decimal strings on the wire;
//! deserialization accepts both strings and numbers (see
//! [`de_f64`]/[`de_i64`]).

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::error::MdkitError;

const REST_BASE: &str = "https://api.hyperliquid.xyz";

/// One entry of the perpetual universe (`meta.universe`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HlUniverseEntry {
    /// Coin symbol, e.g. `BTC` (this is HL's `coin` id elsewhere).
    pub name: String,
    /// Whether the perp has been delisted (still listed historically).
    #[serde(rename = "isDelisted", default)]
    pub is_delisted: bool,
}

/// Perpetual universe metadata (`meta` half of `metaAndAssetCtxs`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HlMeta {
    /// All perpetual assets, index-aligned with the asset-ctx array.
    pub universe: Vec<HlUniverseEntry>,
}

/// Live per-asset context for one perpetual market.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HlAssetCtx {
    /// Current hourly funding rate (wire value is a decimal string).
    #[serde(deserialize_with = "de_f64")]
    pub funding: f64,
    /// Open interest in base units (wire `openInterest`, decimal string).
    #[serde(rename = "openInterest", deserialize_with = "de_f64")]
    pub open_interest: f64,
    /// Mark price (wire `markPx`, decimal string).
    #[serde(rename = "markPx", deserialize_with = "de_f64")]
    pub mark: f64,
    /// Previous day close (wire `prevDayPx`, decimal string).
    #[serde(rename = "prevDayPx", deserialize_with = "de_f64")]
    pub prev_close: f64,
    /// Rolling 24h notional volume (wire `dayNtlVlm`, decimal string).
    #[serde(rename = "dayNtlVlm", deserialize_with = "de_f64")]
    pub day_volume: f64,
}

/// Raw `metaAndAssetCtxs` payload: universe metadata paired with the
/// index-aligned live contexts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HlUniverseSnapshot {
    /// Universe metadata.
    pub meta: HlMeta,
    /// Live contexts, index-aligned with [`HlMeta::universe`].
    pub asset_ctxs: Vec<HlAssetCtx>,
}

/// A universe entry zipped with its live context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HlMarketCtx {
    /// Coin symbol (from [`HlUniverseEntry::name`]).
    pub coin: String,
    /// Live context for the market.
    pub ctx: HlAssetCtx,
    /// Delisted flag passthrough — the caller filters.
    pub is_delisted: bool,
}

/// One row of funding-rate history for a coin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HlFundingRow {
    /// Coin symbol the row belongs to.
    pub coin: String,
    /// Row timestamp in epoch milliseconds (wire `time`).
    #[serde(rename = "time", deserialize_with = "de_i64")]
    pub time_ms: i64,
    /// Hourly funding rate (wire `fundingRate`, decimal string).
    #[serde(rename = "fundingRate", deserialize_with = "de_f64")]
    pub funding_rate: f64,
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

/// Runs one `POST /info` query and parses the JSON response.
async fn info_query(client: &fetch_kit::Client, body: &Value) -> Result<Value, MdkitError> {
    client
        .post(format!("{REST_BASE}/info"))
        .json(body)
        .json_response::<Value>()
        .await
        .map_err(MdkitError::Fetch)
}

/// Zips a raw [`HlUniverseSnapshot`] into a flat per-market list.
/// Nothing is filtered — delisted entries are passed through with
/// [`HlMarketCtx::is_delisted`] set, for the caller to filter.
fn zip_snapshot(snapshot: HlUniverseSnapshot) -> Result<Vec<HlMarketCtx>, MdkitError> {
    if snapshot.meta.universe.len() != snapshot.asset_ctxs.len() {
        return Err(MdkitError::Malformed(format!(
            "universe/ctx length mismatch: {} entries vs {} contexts",
            snapshot.meta.universe.len(),
            snapshot.asset_ctxs.len()
        )));
    }
    let entries = snapshot.meta.universe.into_iter();
    let ctxs = snapshot.asset_ctxs.into_iter();
    Ok(entries
        .zip(ctxs)
        .map(|(entry, ctx)| HlMarketCtx {
            coin: entry.name,
            ctx,
            is_delisted: entry.is_delisted,
        })
        .collect())
}

/// Fetches the full perpetual universe with live contexts as a flat
/// [`HlMarketCtx`] list (delisted entries included; filter on
/// [`HlMarketCtx::is_delisted`]).
///
/// # Errors
/// Returns [`MdkitError`] on HTTP or normalization failure, including
/// universe/context length mismatch.
pub async fn universe_snapshot(client: &fetch_kit::Client) -> Result<Vec<HlMarketCtx>, MdkitError> {
    let value = info_query(client, &serde_json::json!({ "type": "metaAndAssetCtxs" })).await?;
    let pair = value
        .as_array()
        .ok_or_else(|| MdkitError::Malformed("metaAndAssetCtxs payload is not a 2-array".into()))?;
    let meta = pair
        .first()
        .cloned()
        .ok_or_else(|| MdkitError::Malformed("metaAndAssetCtxs payload is empty".into()))?;
    let ctxs = pair
        .get(1)
        .cloned()
        .ok_or_else(|| MdkitError::Malformed("metaAndAssetCtxs payload missing ctxs".into()))?;
    let snapshot = HlUniverseSnapshot {
        meta: serde_json::from_value::<HlMeta>(meta)
            .map_err(|e| MdkitError::Malformed(format!("meta parse failed: {e}")))?,
        asset_ctxs: serde_json::from_value::<Vec<HlAssetCtx>>(ctxs)
            .map_err(|e| MdkitError::Malformed(format!("asset ctxs parse failed: {e}")))?,
    };
    zip_snapshot(snapshot)
}

/// Fetches hourly funding-rate history for `coin` over
/// `[start_ms, end_ms]` epoch milliseconds.
///
/// # Errors
/// Returns [`MdkitError`] on HTTP or normalization failure.
pub async fn funding_history(
    client: &fetch_kit::Client,
    coin: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<HlFundingRow>, MdkitError> {
    let value = info_query(
        client,
        &serde_json::json!({
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
            "endTime": end_ms,
        }),
    )
    .await?;
    serde_json::from_value::<Vec<HlFundingRow>>(value)
        .map_err(|e| MdkitError::Malformed(format!("fundingHistory parse failed: {e}")))
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    #[test]
    fn meta_and_asset_ctxs_2array_parses_and_zips() {
        let raw = serde_json::json!([
            { "universe": [
                { "name": "BTC", "isDelisted": false },
                { "name": "OLD", "isDelisted": true },
            ]},
            [
                {
                    "funding": "0.0000125",
                    "openInterest": "18432.5",
                    "markPx": "97452.3",
                    "prevDayPx": "96100.1",
                    "dayNtlVlm": "503214789.25",
                },
                {
                    "funding": "0.0",
                    "openInterest": "0.0",
                    "markPx": "1.0",
                    "prevDayPx": "1.0",
                    "dayNtlVlm": "0.0",
                },
            ],
        ]);
        let pair = raw.as_array().expect("test payload is an array");
        let snapshot = HlUniverseSnapshot {
            meta: serde_json::from_value::<HlMeta>(pair.first().cloned().expect("meta"))
                .expect("meta parses"),
            asset_ctxs: serde_json::from_value::<Vec<HlAssetCtx>>(
                pair.get(1).cloned().expect("ctxs"),
            )
            .expect("ctxs parse"),
        };
        let markets = zip_snapshot(snapshot).expect("zip succeeds");
        assert_eq!(markets.len(), 2);

        assert_eq!(markets.first().map(|m| &*m.coin), Some("BTC"));
        let btc = &markets[0];
        assert!(!btc.is_delisted);
        assert_eq!(btc.ctx.funding, 0.000_012_5);
        assert_eq!(btc.ctx.open_interest, 18_432.5);
        assert_eq!(btc.ctx.mark, 97_452.3);
        assert_eq!(btc.ctx.prev_close, 96_100.1);
        assert_eq!(btc.ctx.day_volume, 503_214_789.25);

        let old = &markets[1];
        assert_eq!(old.coin, "OLD");
        assert!(old.is_delisted);
    }

    #[test]
    fn zip_rejects_length_mismatch() {
        let snapshot = HlUniverseSnapshot {
            meta: HlMeta {
                universe: vec![HlUniverseEntry {
                    name: "BTC".into(),
                    is_delisted: false,
                }],
            },
            asset_ctxs: vec![],
        };
        assert!(zip_snapshot(snapshot).is_err());
    }

    #[test]
    fn funding_history_array_parses_string_numerics() {
        let raw = serde_json::json!([
            { "coin": "BTC", "time": "1731604800000", "fundingRate": "0.00001250" },
            { "coin": "BTC", "time": 1_731_608_400_000_i64, "fundingRate": "-0.00000875" },
        ]);
        let rows = serde_json::from_value::<Vec<HlFundingRow>>(raw).expect("funding rows parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].coin, "BTC");
        assert_eq!(rows[0].time_ms, 1_731_604_800_000);
        assert_eq!(rows[0].funding_rate, 0.000_012_5);
        assert_eq!(rows[1].time_ms, 1_731_608_400_000);
        assert_eq!(rows[1].funding_rate, -0.000_008_75);
    }

    #[test]
    fn delisted_flag_defaults_and_passes_through() {
        // Absent isDelisted (spot-style rows) defaults to false.
        let entry: HlUniverseEntry =
            serde_json::from_value(serde_json::json!({ "name": "NEW" })).expect("entry parses");
        assert!(!entry.is_delisted);
        let entry: HlUniverseEntry = serde_json::from_value(serde_json::json!(
            { "name": "OLD", "isDelisted": true }
        ))
        .expect("entry parses");
        assert!(entry.is_delisted);
    }
}
