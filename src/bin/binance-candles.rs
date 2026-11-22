//! `binance-candles` — fetch recent Binance klines into a JSONL tape.
//!
//! Calls [`mdkit::binance::klines`] with a breaker-protected
//! [`fetch_kit::Client`] and writes one JSON line per candle (oldest
//! first), each a [`mdkit::Candle`] object, to stdout or `--out`.
//!
//! Exit is 2 for bad arguments and 1 for fetch or I/O failures.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::process::ExitCode;
use std::time::Duration;

use mdkit::binance::klines;
use mdkit::Candle;

const USAGE: &str =
    "usage: binance-candles --symbol BTCUSDT --interval 1h --limit 500 [--out out.jsonl]";

/// Binance `/api/v3/klines` hard cap on `limit`.
const MAX_LIMIT: u32 = 1000;

/// Parsed command-line arguments.
struct Args {
    /// Trading pair, e.g. `BTCUSDT`.
    symbol: String,
    /// Kline interval, e.g. `1h`.
    interval: String,
    /// Number of candles to fetch (1..=1000).
    limit: u32,
    /// Output path; `None` writes to stdout.
    out: Option<String>,
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{USAGE}\nbinance-candles: {message}");
            return ExitCode::from(2);
        }
    };
    run(&args)
}

/// Fetches the klines and writes the JSONL tape.
fn run(args: &Args) -> ExitCode {
    let client = fetch_kit::Client::builder()
        .user_agent("binance-candles/0.1")
        .timeout(Duration::from_secs(10))
        .retries(2)
        .build();

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("binance-candles: runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    let candles = match runtime.block_on(klines(&client, &args.symbol, &args.interval, args.limit))
    {
        Ok(candles) => candles,
        Err(error) => {
            eprintln!("binance-candles: {error}");
            return ExitCode::FAILURE;
        }
    };

    let writer = match open_writer(args.out.as_deref()) {
        Ok(writer) => writer,
        Err(error) => {
            eprintln!(
                "binance-candles: {}: {error}",
                args.out.as_deref().unwrap_or("stdout")
            );
            return ExitCode::FAILURE;
        }
    };
    match write_tape(writer, &candles) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("binance-candles: write: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Writer over `--out`, defaulting to a locked stdout.
fn open_writer(out: Option<&str>) -> std::io::Result<BufWriter<Box<dyn Write>>> {
    match out {
        Some(path) => Ok(BufWriter::new(Box::new(File::create(path)?))),
        None => Ok(BufWriter::new(Box::new(std::io::stdout().lock()))),
    }
}

/// Writes one JSON line per candle and flushes.
fn write_tape(mut writer: BufWriter<Box<dyn Write>>, candles: &[Candle]) -> std::io::Result<()> {
    for candle in candles {
        serde_json::to_writer(&mut writer, candle)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()
}

/// Parses `--flag value` pairs; `--out` is optional, the rest required.
fn parse_args<I>(mut args: I) -> Result<Args, String>
where
    I: Iterator<Item = String>,
{
    let (mut symbol, mut interval, mut limit, mut out) = (None, None, None, None);
    while let Some(arg) = args.next() {
        let slot: Option<&mut Option<String>> = match arg.as_str() {
            "--symbol" => Some(&mut symbol),
            "--interval" => Some(&mut interval),
            "--limit" => Some(&mut limit),
            "--out" => Some(&mut out),
            other => return Err(format!("unrecognized argument `{other}`")),
        };
        let Some(slot) = slot else {
            continue;
        };
        if slot.is_some() {
            return Err(format!("`{arg}` given more than once"));
        }
        *slot = Some(
            args.next()
                .ok_or_else(|| format!("`{arg}` requires a value"))?,
        );
    }
    let limit = limit
        .ok_or("missing --limit")?
        .trim()
        .parse::<u32>()
        .map_err(|_| "invalid --limit: expected an integer".to_string())?;
    if limit == 0 || limit > MAX_LIMIT {
        return Err(format!("--limit must be between 1 and {MAX_LIMIT}"));
    }
    Ok(Args {
        symbol: non_empty(symbol, "--symbol")?,
        interval: non_empty(interval, "--interval")?,
        limit,
        out,
    })
}

/// Unwraps a required flag slot, rejecting empty values.
fn non_empty(slot: Option<String>, flag: &str) -> Result<String, String> {
    match slot {
        Some(value) if !value.is_empty() => Ok(value),
        Some(_) => Err(format!("{flag} requires a non-empty value")),
        None => Err(format!("missing {flag}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Result<Args, String> {
        parse_args(list.iter().copied().map(String::from))
    }

    #[test]
    fn parses_full_arg_set() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = args(&["--symbol", "BTCUSDT", "--interval", "1h", "--limit", "500"])?;
        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.interval, "1h");
        assert_eq!(parsed.limit, 500);
        assert_eq!(parsed.out, None);
        Ok(())
    }

    #[test]
    fn parses_optional_out() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = args(&[
            "--symbol",
            "ETHUSDT",
            "--interval",
            "1m",
            "--limit",
            "10",
            "--out",
            "tape.jsonl",
        ])?;
        assert_eq!(parsed.out, Some("tape.jsonl".to_string()));
        Ok(())
    }

    #[test]
    fn rejects_unknown_flag() {
        assert!(args(&["--symbol", "BTCUSDT", "--wat", "1"]).is_err());
    }

    #[test]
    fn rejects_missing_required() {
        assert!(args(&["--interval", "1h", "--limit", "5"]).is_err());
        assert!(args(&["--symbol", "BTCUSDT", "--limit", "5"]).is_err());
        assert!(args(&["--symbol", "BTCUSDT", "--interval", "1h"]).is_err());
    }

    #[test]
    fn rejects_empty_values() {
        assert!(args(&["--symbol", "", "--interval", "1h", "--limit", "5"]).is_err());
        assert!(args(&["--symbol", "BTCUSDT", "--interval", "", "--limit", "5"]).is_err());
    }

    #[test]
    fn rejects_bad_limit() {
        assert!(args(&["--symbol", "BTCUSDT", "--interval", "1h", "--limit", "abc"]).is_err());
        assert!(args(&["--symbol", "BTCUSDT", "--interval", "1h", "--limit", "0"]).is_err());
        assert!(args(&["--symbol", "BTCUSDT", "--interval", "1h", "--limit", "1001"]).is_err());
    }

    #[test]
    fn rejects_duplicate_flag() {
        assert!(args(&[
            "--symbol",
            "BTCUSDT",
            "--symbol",
            "ETHUSDT",
            "--interval",
            "1h",
            "--limit",
            "5"
        ])
        .is_err());
    }

    #[test]
    fn rejects_missing_value() {
        assert!(args(&["--symbol", "BTCUSDT", "--interval", "1h", "--limit"]).is_err());
    }
}
