// SPDX-License-Identifier: Apache-2.0
#![no_main]

//! RS-5: the traceql parser over arbitrary text.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = tessera_traceql::parse(&String::from_utf8_lossy(data));
});
