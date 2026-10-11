// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! What the node tests share.

use std::time::Duration;

/// Polls `condition` every fifty milliseconds for up to `patience`.
pub async fn eventually(what: &str, patience: Duration, mut condition: impl FnMut() -> bool) {
    let started = tokio::time::Instant::now();
    while started.elapsed() < patience {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what} did not happen within {patience:?}");
}

/// Posts `body` as JSON to `url` with `headers`, returning the status
/// and the body.
pub async fn post(
    url: String,
    headers: Vec<(String, String)>,
    body: serde_json::Value,
) -> (u16, String) {
    tokio::task::spawn_blocking(move || {
        let mut request = ureq::post(&url)
            .config()
            .http_status_as_error(false)
            .build();
        for (name, value) in &headers {
            request = request.header(name, value);
        }
        let mut response = request.send_json(body).unwrap();
        let status = response.status().as_u16();
        (status, response.body_mut().read_to_string().unwrap())
    })
    .await
    .unwrap()
}
