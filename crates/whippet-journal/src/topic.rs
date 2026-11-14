// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::fmt;
use std::str::FromStr;

use iggy::prelude::Durability;
use whippet_core::config::ExchangeConfig;
use whippet_core::error::{CoreError, Result};

use crate::{STREAM_MD, STREAM_OPS, STREAM_TRADING};

/// The pinned topic layout. Partition counts, durability, and retention are
/// part of the product contract, not per-deployment surprises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Topic {
    Orders,
    Executions,
    Md,
    Alerts,
    Audit,
    Commands,
}

impl Topic {
    pub const ALL: [Topic; 6] = [
        Topic::Orders,
        Topic::Executions,
        Topic::Md,
        Topic::Alerts,
        Topic::Audit,
        Topic::Commands,
    ];

    pub fn stream(self) -> &'static str {
        match self {
            Self::Orders | Self::Executions => STREAM_TRADING,
            Self::Md => STREAM_MD,
            Self::Alerts | Self::Audit | Self::Commands => STREAM_OPS,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Orders => "orders",
            Self::Executions => "executions",
            Self::Md => "md",
            Self::Alerts => "alerts",
            Self::Audit => "audit",
            Self::Commands => "commands",
        }
    }

    pub fn partition_count(self, exchange: &ExchangeConfig) -> u32 {
        match self {
            Self::Orders | Self::Executions | Self::Md => exchange.partitions,
            Self::Alerts | Self::Audit => 4,
            Self::Commands => 1,
        }
    }

    pub fn durability(self) -> Durability {
        match self {
            Self::Md => Durability::Replicated,
            _ => Durability::Persisted,
        }
    }

    pub fn consumer_offset_durability(self) -> Durability {
        match self {
            Self::Md => Durability::Replicated,
            _ => Durability::Persisted,
        }
    }

    pub fn retention_secs(self) -> u64 {
        match self {
            Self::Orders => 24 * 60 * 60,
            Self::Executions => 7 * 24 * 60 * 60,
            Self::Md => 60 * 60,
            Self::Alerts => 30 * 24 * 60 * 60,
            Self::Audit => 90 * 24 * 60 * 60,
            Self::Commands => 7 * 24 * 60 * 60,
        }
    }

    pub fn key(self) -> &'static str {
        self.name()
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.stream(), self.name())
    }
}

impl FromStr for Topic {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "orders" => Ok(Self::Orders),
            "executions" => Ok(Self::Executions),
            "md" => Ok(Self::Md),
            "alerts" => Ok(Self::Alerts),
            "audit" => Ok(Self::Audit),
            "commands" => Ok(Self::Commands),
            other => Err(CoreError::InvalidValue(format!("unknown topic: {other}"))),
        }
    }
}
