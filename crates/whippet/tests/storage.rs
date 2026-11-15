// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Bounded-storage guard: threshold classification, configuration validation,
//! and real free-space sampling on a temporary directory.
//!
//! Nothing here needs a nearly-full disk: the classification boundaries are a
//! pure function and the monitor is exercised against whatever filesystem
//! holds the temp directory.

use clap::Parser;
use whippet::config::NodeConfig;
use whippet::storage::{
    DEFAULT_CRIT_PCT, DEFAULT_WARN_PCT, StorageConfig, StorageMonitor, StorageState, classify,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    node: whippet::config::NodeArgs,
}

fn config_from(args: &[&str]) -> Result<NodeConfig, whippet::config::ConfigError> {
    let cli = Cli::parse_from(std::iter::once("test").chain(args.iter().copied()));
    NodeConfig::from_args(cli.node)
}

#[test]
fn classify_boundaries_match_the_documented_defaults() {
    assert_eq!(DEFAULT_WARN_PCT, 70);
    assert_eq!(DEFAULT_CRIT_PCT, 85);

    // The percentage in each pair is the volume usage; the expected state is
    // Ok up to just below the warning, Warn in the warning band (both ends),
    // and Critical from the critical threshold up.
    let cases = [
        (0, StorageState::Ok),
        (69, StorageState::Ok),
        (70, StorageState::Warn),
        (84, StorageState::Warn),
        (85, StorageState::Critical),
        (100, StorageState::Critical),
    ];
    for (used_pct, expected) in cases {
        assert_eq!(
            classify(used_pct, DEFAULT_WARN_PCT, DEFAULT_CRIT_PCT),
            expected,
            "{used_pct}% must classify as {expected:?}"
        );
    }
}

#[test]
fn classify_uses_the_configured_thresholds() {
    assert_eq!(classify(50, 51, 90), StorageState::Ok);
    assert_eq!(classify(51, 51, 90), StorageState::Warn);
    assert_eq!(classify(89, 51, 90), StorageState::Warn);
    assert_eq!(classify(90, 51, 90), StorageState::Critical);
}

#[test]
fn thresholds_and_order_are_validated_at_startup() {
    // warn >= crit, and either threshold outside 1..=99.
    for args in [
        ["--disk-warn-pct", "70", "--disk-crit-pct", "70"].as_slice(),
        ["--disk-warn-pct", "90", "--disk-crit-pct", "85"].as_slice(),
        ["--disk-warn-pct", "0", "--disk-crit-pct", "85"].as_slice(),
        ["--disk-warn-pct", "70", "--disk-crit-pct", "100"].as_slice(),
        ["--disk-warn-pct", "100", "--disk-crit-pct", "100"].as_slice(),
    ] {
        assert!(
            config_from(args).is_err(),
            "{args:?} must be rejected at startup"
        );
    }

    let config = config_from(&["--disk-warn-pct", "60", "--disk-crit-pct", "80"])
        .expect("60/80 is a valid pair");
    assert_eq!(config.storage.warn_pct, 60);
    assert_eq!(config.storage.crit_pct, 80);
}

#[test]
fn the_defaults_are_the_documented_one() {
    let config = config_from(&[]).expect("defaults are valid");
    assert_eq!(config.storage.warn_pct, DEFAULT_WARN_PCT);
    assert_eq!(config.storage.crit_pct, DEFAULT_CRIT_PCT);
    assert_eq!(config.storage.whippet_dir, std::path::Path::new("/data"));
}

#[test]
fn a_fresh_monitor_reports_a_plausible_volume() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = StorageConfig {
        whippet_dir: dir.path().to_path_buf(),
        iggy_dir: None,
        ..StorageConfig::default()
    };
    let monitor = StorageMonitor::new(config);

    let snapshot = monitor.snapshot();
    assert!(snapshot.whippet.available, "the temp dir must be statable");
    assert_eq!(snapshot.whippet.name, "whippet");
    assert!(
        snapshot.whippet.total_bytes > 0,
        "a real filesystem reports a non-zero size"
    );
    assert!(
        snapshot.whippet.free_bytes <= snapshot.whippet.total_bytes,
        "free bytes cannot exceed the filesystem size"
    );
    assert!(snapshot.iggy.is_none());
    assert!(snapshot.whippet.used_pct <= 100, "usage is a percentage");
    // Whatever the host disk holds, the state must agree with the reading and
    // the configured thresholds rather than with a hard-coded expectation.
    assert_eq!(
        snapshot.state,
        classify(
            snapshot.whippet.used_pct,
            DEFAULT_WARN_PCT,
            DEFAULT_CRIT_PCT
        )
    );
    assert_eq!(monitor.state(), snapshot.state);
    assert_eq!(monitor.config().warn_pct, DEFAULT_WARN_PCT);

    // A refresh re-samples and keeps the API cheap and copyable.
    let refreshed = monitor.refresh();
    assert!(refreshed.whippet.available);
    assert!(refreshed.checked_at >= snapshot.checked_at);
}

#[test]
fn a_missing_iggy_volume_is_absent_rather_than_fatal() {
    let dir = tempfile::tempdir().expect("temp dir");
    let missing = dir.path().join("not-mounted");
    assert!(!missing.exists());

    // The config layer turns a missing path into `None`, so the monitor reports
    // only the Whippet volume instead of sampling an absent mount forever.
    let config = config_from(&[
        "--data-dir",
        dir.path().to_str().expect("utf-8 path"),
        "--iggy-data-dir",
        missing.to_str().expect("utf-8 path"),
    ])
    .expect("a missing Iggy volume is not a startup error");
    assert_eq!(config.storage.iggy_dir, None);

    let monitor = StorageMonitor::new(config.storage.clone());
    let snapshot = monitor.snapshot();
    assert!(snapshot.iggy.is_none());
    assert!(snapshot.whippet.available);
    assert_eq!(
        snapshot.state,
        classify(
            snapshot.whippet.used_pct,
            config.storage.warn_pct,
            config.storage.crit_pct
        )
    );
}

#[test]
fn an_absent_whippet_directory_is_unavailable_not_a_panic() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = StorageConfig {
        whippet_dir: dir.path().join("absent"),
        iggy_dir: None,
        ..StorageConfig::default()
    };
    let monitor = StorageMonitor::new(config);
    let snapshot = monitor.refresh();
    assert!(!snapshot.whippet.available);
    assert_eq!(snapshot.whippet.used_pct, 0);
    // A stat failure is a warning: it must not block ingress on its own.
    assert_eq!(snapshot.state, StorageState::Warn);
}

#[test]
fn a_configured_iggy_volume_is_sampled() {
    let dir = tempfile::tempdir().expect("temp dir");
    let iggy = dir.path().join("iggy");
    std::fs::create_dir_all(&iggy).expect("iggy dir");
    let config = StorageConfig {
        whippet_dir: dir.path().to_path_buf(),
        iggy_dir: Some(iggy),
        ..StorageConfig::default()
    };
    let monitor = StorageMonitor::new(config);
    let snapshot = monitor.snapshot();
    let iggy = snapshot.iggy.expect("the configured volume is sampled");
    assert_eq!(iggy.name, "iggy");
    assert!(iggy.available);
    assert!(iggy.total_bytes > 0);
    assert!(iggy.used_pct <= 100);
}
