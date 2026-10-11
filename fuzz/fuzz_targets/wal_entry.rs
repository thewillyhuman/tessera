// SPDX-License-Identifier: LicenseRef-Tessera-Pending
#![no_main]

//! RS-5: the writer-log entry decoder over arbitrary bytes.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = tessera_wal::Entry::decode(&bytes::Bytes::copy_from_slice(data));
});
