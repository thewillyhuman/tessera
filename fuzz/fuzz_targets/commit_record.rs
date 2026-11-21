// SPDX-License-Identifier: Apache-2.0
#![no_main]

//! RS-5: the commit record decoder over arbitrary bytes.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = tessera_commit::Commit::decode(data);
});
