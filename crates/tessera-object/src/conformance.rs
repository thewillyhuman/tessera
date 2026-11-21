// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The contract every `ObjectStore` implementation must honour (ST-3),
//! as executable checks. Each implementation's tests call [`run_all`].

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;

use crate::{Key, ObjectError, ObjectStore};

/// Runs every conformance check against `store`. Checks use disjoint key
/// prefixes, so one store instance serves them all.
///
/// # Panics
///
/// On the first check that fails, with the name of the check.
pub async fn run_all(store: Arc<dyn ObjectStore>) {
    get_missing_is_not_found(&*store).await;
    put_then_get_round_trips(&*store).await;
    put_replaces_an_existing_object(&*store).await;
    put_if_absent_creates_once_and_keeps_the_first_body(&*store).await;
    get_range_returns_the_requested_bytes(&*store).await;
    get_range_outside_the_object_is_rejected(&*store).await;
    head_reports_the_size(&*store).await;
    list_returns_keys_under_the_prefix_in_order(&*store).await;
    list_names_gives_each_name_under_the_prefix_once(&*store).await;
    delete_removes_the_object_and_is_idempotent(&*store).await;
    concurrent_put_if_absent_has_one_winner(Arc::clone(&store)).await;
}

fn key(text: &str) -> Key {
    Key::new(text).expect("conformance keys are valid")
}

async fn get_missing_is_not_found(store: &dyn ObjectStore) {
    let error = store.get(&key("c1/missing")).await.unwrap_err();
    assert!(
        matches!(error, ObjectError::NotFound { .. }),
        "get_missing_is_not_found: {error}"
    );
}

async fn put_then_get_round_trips(store: &dyn ObjectStore) {
    store
        .put(&key("c2/a"), Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let body = store.get(&key("c2/a")).await.unwrap();
    assert_eq!(
        body,
        Bytes::from_static(b"hello"),
        "put_then_get_round_trips"
    );
}

async fn put_replaces_an_existing_object(store: &dyn ObjectStore) {
    store
        .put(&key("c3/a"), Bytes::from_static(b"one"))
        .await
        .unwrap();
    store
        .put(&key("c3/a"), Bytes::from_static(b"two"))
        .await
        .unwrap();
    let body = store.get(&key("c3/a")).await.unwrap();
    assert_eq!(
        body,
        Bytes::from_static(b"two"),
        "put_replaces_an_existing_object"
    );
}

async fn put_if_absent_creates_once_and_keeps_the_first_body(store: &dyn ObjectStore) {
    store
        .put_if_absent(&key("c4/a"), Bytes::from_static(b"first"))
        .await
        .unwrap();
    let error = store
        .put_if_absent(&key("c4/a"), Bytes::from_static(b"second"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, ObjectError::AlreadyExists { .. }),
        "put_if_absent second call: {error}"
    );
    let body = store.get(&key("c4/a")).await.unwrap();
    assert_eq!(
        body,
        Bytes::from_static(b"first"),
        "put_if_absent keeps the first body"
    );
}

async fn get_range_returns_the_requested_bytes(store: &dyn ObjectStore) {
    store
        .put(&key("c5/a"), Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    let body = store.get_range(&key("c5/a"), 2..5).await.unwrap();
    assert_eq!(
        body,
        Bytes::from_static(b"234"),
        "get_range_returns_the_requested_bytes"
    );
    let empty = store.get_range(&key("c5/a"), 10..10).await.unwrap();
    assert!(empty.is_empty(), "empty range at the end is allowed");
}

async fn get_range_outside_the_object_is_rejected(store: &dyn ObjectStore) {
    store
        .put(&key("c6/a"), Bytes::from_static(b"0123"))
        .await
        .unwrap();
    let error = store.get_range(&key("c6/a"), 2..9).await.unwrap_err();
    assert!(
        matches!(error, ObjectError::InvalidRange { size: 4, .. }),
        "range past the end: {error}"
    );
    let reversed = std::ops::Range { start: 3, end: 2 };
    let error = store.get_range(&key("c6/a"), reversed).await.unwrap_err();
    assert!(
        matches!(error, ObjectError::InvalidRange { .. }),
        "reversed range: {error}"
    );
    let error = store.get_range(&key("c6/missing"), 0..1).await.unwrap_err();
    assert!(
        matches!(error, ObjectError::NotFound { .. }),
        "range of missing object: {error}"
    );
}

async fn head_reports_the_size(store: &dyn ObjectStore) {
    store
        .put(&key("c7/a"), Bytes::from_static(b"sized"))
        .await
        .unwrap();
    let meta = store.head(&key("c7/a")).await.unwrap();
    assert_eq!(
        (meta.key, meta.size),
        (key("c7/a"), 5),
        "head_reports_the_size"
    );
    let error = store.head(&key("c7/missing")).await.unwrap_err();
    assert!(
        matches!(error, ObjectError::NotFound { .. }),
        "head of missing object: {error}"
    );
}

async fn list_returns_keys_under_the_prefix_in_order(store: &dyn ObjectStore) {
    for name in ["c8/c/2", "c8/c/1", "c8/c/x/3", "c8/cx/4"] {
        store
            .put(&key(name), Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    let listed: Vec<Key> = store
        .list(&key("c8/c"))
        .map_ok(|meta| meta.key)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        listed,
        [key("c8/c/1"), key("c8/c/2"), key("c8/c/x/3")],
        "list under c8/c"
    );
    let nested: Vec<Key> = store
        .list(&key("c8/c/x"))
        .map_ok(|meta| meta.key)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(nested, [key("c8/c/x/3")], "list under c8/c/x");
    let none: Vec<Key> = store
        .list(&key("c8/missing"))
        .map_ok(|meta| meta.key)
        .try_collect()
        .await
        .unwrap();
    assert!(none.is_empty(), "list under a missing prefix is empty");
}

async fn list_names_gives_each_name_under_the_prefix_once(store: &dyn ObjectStore) {
    for name in [
        "c11/b/2",
        "c11/a/1",
        "c11/a/x/3",
        "c11/a-b/4",
        "c11/c",
        "c11x/d",
    ] {
        store
            .put(&key(name), Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    let names = store.list_names(&key("c11")).await.unwrap();
    assert_eq!(names, ["a", "a-b", "b", "c"], "names under c11");
    let nested = store.list_names(&key("c11/a")).await.unwrap();
    assert_eq!(nested, ["1", "x"], "names under c11/a");
    let none = store.list_names(&key("c11/missing")).await.unwrap();
    assert!(none.is_empty(), "no names under a missing prefix");
}

async fn delete_removes_the_object_and_is_idempotent(store: &dyn ObjectStore) {
    store
        .put(&key("c9/a"), Bytes::from_static(b"x"))
        .await
        .unwrap();
    store.delete(&key("c9/a")).await.unwrap();
    let error = store.get(&key("c9/a")).await.unwrap_err();
    assert!(
        matches!(error, ObjectError::NotFound { .. }),
        "deleted object is gone: {error}"
    );
    store
        .delete(&key("c9/a"))
        .await
        .expect("deleting a missing object succeeds");
    store
        .put_if_absent(&key("c9/a"), Bytes::from_static(b"y"))
        .await
        .expect("key is free again");
}

async fn concurrent_put_if_absent_has_one_winner(store: Arc<dyn ObjectStore>) {
    let mut tasks = Vec::new();
    for i in 0..16u8 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            store
                .put_if_absent(&key("c10/a"), Bytes::from(vec![i]))
                .await
                .is_ok()
        }));
    }
    let mut winners = 0;
    for task in tasks {
        if task.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "concurrent_put_if_absent_has_one_winner");
}
