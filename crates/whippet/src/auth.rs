// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Ingress credential helpers.

use serde::{Deserialize, Serialize};

/// One bearer token bound to a participant identity.
///
/// The participant is what the matcher uses for self-trade prevention and
/// order attribution. Binding it to the token means two clients cannot share
/// one identity by accident, and a client cannot claim another participant by
/// putting a different name in the request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiToken {
    pub token: String,
    pub participant: String,
}

/// Compare without early exit so a wrong token cannot be recovered byte by
/// byte from response timing.
#[inline]
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right) {
        diff |= a ^ b;
    }
    diff == 0
}
