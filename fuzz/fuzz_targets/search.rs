// SPDX-License-Identifier: Apache-2.0
#![no_main]

//! RS-5: the search syntax parser over arbitrary text, for every kind.

use libfuzzer_sys::fuzz_target;
use tessera_types::Kind;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    for kind in Kind::ALL {
        let _ = tessera_search::parse(&text, kind);
    }
});
