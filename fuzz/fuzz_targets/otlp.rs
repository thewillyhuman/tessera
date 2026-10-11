// SPDX-License-Identifier: LicenseRef-Tessera-Pending
#![no_main]

//! RS-5: the OTLP decoders, JSON and protobuf, for every signal, and
//! the conversions of what decodes.

use libfuzzer_sys::fuzz_target;
use tessera_ingest::otlp;
use tessera_types::{DatasetName, SeededRandom, TenantId, Timestamp};

fuzz_target!(|data: &[u8]| {
    let dataset = DatasetName::new("prod").unwrap();
    let now = Timestamp::from_nanos(1);
    let mut random = SeededRandom::new(1);
    for request in [otlp::parse_json(data), otlp::parse_protobuf(data)].into_iter().flatten() {
        let _ = otlp::to_batch(&request, TenantId::DEFAULT, dataset.clone(), now, &mut random);
    }
    for request in [otlp::traces::parse_json(data), otlp::traces::parse_protobuf(data)]
        .into_iter()
        .flatten()
    {
        let _ = otlp::traces::to_batches(&request, TenantId::DEFAULT, dataset.clone(), now);
    }
    for request in [otlp::metrics::parse_json(data), otlp::metrics::parse_protobuf(data)]
        .into_iter()
        .flatten()
    {
        let _ = otlp::metrics::to_batch(&request, TenantId::DEFAULT, dataset.clone(), now);
    }
});
