// SPDX-License-Identifier: Apache-2.0

//! The object layout (spec section 6.2): the one place every key is built.
//!
//! ```text
//! root pool
//!   _cluster/catalog/_log/{version:020}          cluster catalog
//!   _cluster/leases/{kind}/{slot}/{epoch}        slot leases
//!   _cluster/nodes/{node_id}                     node registry
//!   _cluster/budgets/{node_id}                   budget counts a node published
//!
//! any pool
//!   _wal/{writer_id}/{seq:012}                   writer log entries
//!   {tenant}/_control/_log/{version:020}         tenant control store
//!   {tenant}/_eval/{group}/{part}/_log/{version:020}    rule state log
//!   {tenant}/_eval/{group}/_split/{name}                re-split checkpoint
//!   {tenant}/_eval/{group}/_partials/{parts}/{part}/{writer_id}/{seq:012}
//!                                                partial windows of log entries up to seq
//!   {tenant}/{dataset}/_log/{version:020}        dataset log
//!   {tenant}/{dataset}/_writers/{writer_id}      writer registry
//!   {tenant}/{dataset}/p/{kind}/{window}/_log/{version:020}       partition manifest log
//!   {tenant}/{dataset}/p/{kind}/{window}/d/{hh}/{file_id}.parquet data file
//!   {tenant}/{dataset}/p/{kind}/{window}/i/{hh}/{file_id}.idx     sidecar index
//! ```
//!
//! Commit logs take a prefix and add `_log/{version}` themselves, so the
//! functions here return the prefix for logs and the full key for objects.

use tessera_types::{DatasetName, FileId, NodeId, PartitionId, Seq, TenantId, WriterId};

use crate::Key;

/// Joins validated components; every identifier type guarantees that its
/// text is a valid component, so this cannot fail.
fn key(components: &[&str]) -> Key {
    Key::new(components.join("/"))
        .unwrap_or_else(|e| unreachable!("layout keys are built from validated parts: {e}"))
}

/// Prefix of the cluster catalog log.
#[must_use]
pub fn catalog() -> Key {
    key(&["_cluster", "catalog"])
}

/// Prefix of all slot leases.
#[must_use]
pub fn leases() -> Key {
    key(&["_cluster", "leases"])
}

/// Prefix of the node registry (section 14.1).
#[must_use]
pub fn nodes() -> Key {
    key(&["_cluster", "nodes"])
}

/// The node registry entry of one node.
#[must_use]
pub fn node(node: NodeId) -> Key {
    key(&["_cluster", "nodes", &node.to_string()])
}

/// Prefix of the budget counts the nodes publish (section 13.4).
#[must_use]
pub fn budgets() -> Key {
    key(&["_cluster", "budgets"])
}

/// The budget counts one node published.
#[must_use]
pub fn budget_counts(node: NodeId) -> Key {
    key(&["_cluster", "budgets", &node.to_string()])
}

/// The object a node creates, tries to create again and removes when
/// it starts, to check that the store refuses the second creation
/// ([`crate::probe`]).
#[must_use]
pub fn probe(node: NodeId) -> Key {
    key(&["_cluster", "probes", &node.to_string()])
}

/// Prefix of one writer's log.
#[must_use]
pub fn writer_log(writer: WriterId) -> Key {
    key(&["_wal", &writer.to_string()])
}

/// One entry of a writer's log.
#[must_use]
pub fn writer_log_entry(writer: WriterId, seq: Seq) -> Key {
    key(&["_wal", &writer.to_string(), &seq.to_string()])
}

/// Whether the object at `key` never changes once it is written: a
/// data file, a sidecar index or a writer-log entry. What was read of
/// such an object stays true for as long as the object exists, which
/// is what lets a node keep it on local disk (section 10.8).
#[must_use]
pub fn is_immutable(key: &Key) -> bool {
    let extension = key.name().rsplit_once('.').map(|(_, extension)| extension);
    matches!(extension, Some("parquet" | "idx")) || key.as_str().starts_with("_wal/")
}

/// Prefix of a tenant's control store log.
#[must_use]
pub fn control_log(tenant: &TenantId) -> Key {
    key(&[tenant.as_str(), "_control"])
}

/// Prefix of the state log of one partition of a rule group (CQ-12).
/// The group name is restricted to key characters by the control
/// store's validation.
#[must_use]
pub fn eval_log(tenant: &TenantId, group: &str, part: u32) -> Key {
    key(&[tenant.as_str(), "_eval", group, &part.to_string()])
}

/// The checkpoint partition 0 of a rule group writes when the group's
/// partition count changes (CQ-6), named by the new count and the time
/// of the change; the other partitions take their keys from it.
#[must_use]
pub fn eval_split(tenant: &TenantId, group: &str, name: &str) -> Key {
    key(&[tenant.as_str(), "_eval", group, "_split", name])
}

/// Prefix of every partial window left for a rule group while it had
/// `parts` state partitions, so that a re-split can clear them.
#[must_use]
pub fn eval_partials_of(tenant: &TenantId, group: &str, parts: u32) -> Key {
    key(&[
        tenant.as_str(),
        "_eval",
        group,
        "_partials",
        &parts.to_string(),
    ])
}

/// Prefix of the partial windows the source of `writer`'s log leaves
/// for state partition `part` of the `parts` of a rule group (CQ-4).
#[must_use]
pub fn eval_partials(
    tenant: &TenantId,
    group: &str,
    parts: u32,
    part: u32,
    writer: WriterId,
) -> Key {
    key(&[
        tenant.as_str(),
        "_eval",
        group,
        "_partials",
        &parts.to_string(),
        &part.to_string(),
        &writer.to_string(),
    ])
}

/// The partial windows of a run of entries of `writer`'s log, the
/// last of them `seq`, for state partition `part` of the `parts` of a
/// rule group (CQ-4).
#[must_use]
pub fn eval_partial(
    tenant: &TenantId,
    group: &str,
    parts: u32,
    part: u32,
    writer: WriterId,
    seq: Seq,
) -> Key {
    key(&[
        tenant.as_str(),
        "_eval",
        group,
        "_partials",
        &parts.to_string(),
        &part.to_string(),
        &writer.to_string(),
        &seq.to_string(),
    ])
}

/// Prefix of a dataset's log.
#[must_use]
pub fn dataset_log(tenant: &TenantId, dataset: &DatasetName) -> Key {
    key(&[tenant.as_str(), dataset.as_str()])
}

/// Prefix of a dataset's writer registry.
#[must_use]
pub fn writer_registry(tenant: &TenantId, dataset: &DatasetName) -> Key {
    key(&[tenant.as_str(), dataset.as_str(), "_writers"])
}

/// One writer's entry in a dataset's registry.
#[must_use]
pub fn writer_registration(tenant: &TenantId, dataset: &DatasetName, writer: WriterId) -> Key {
    key(&[
        tenant.as_str(),
        dataset.as_str(),
        "_writers",
        &writer.to_string(),
    ])
}

/// Prefix under which a dataset's partitions of one kind live; listing it
/// discovers the partitions (MF-3).
#[must_use]
pub fn partitions(tenant: &TenantId, dataset: &DatasetName, kind: tessera_types::Kind) -> Key {
    key(&[tenant.as_str(), dataset.as_str(), "p", kind.as_str()])
}

/// Prefix of a dataset's Apache Iceberg export of one kind (OP-9): the
/// table's location, its metadata under `metadata/`.
#[must_use]
pub fn iceberg_table(tenant: &TenantId, dataset: &DatasetName, kind: tessera_types::Kind) -> Key {
    key(&[tenant.as_str(), dataset.as_str(), "iceberg", kind.as_str()])
}

/// Prefix of a partition: its manifest log, data files and sidecars.
#[must_use]
pub fn partition(partition: &PartitionId) -> Key {
    key(&[
        partition.tenant.as_str(),
        partition.dataset.as_str(),
        "p",
        partition.kind.as_str(),
        &partition.window.to_string(),
    ])
}

/// A data file of a partition.
#[must_use]
pub fn data_file(partition: &PartitionId, file: FileId) -> Key {
    self::partition(partition)
        .join("d")
        .and_then(|k| k.join(&file.hash_prefix()))
        .and_then(|k| k.join(&format!("{file}.parquet")))
        .unwrap_or_else(|e| unreachable!("layout keys are built from validated parts: {e}"))
}

/// The sidecar index of a data file.
#[must_use]
pub fn sidecar(partition: &PartitionId, file: FileId) -> Key {
    self::partition(partition)
        .join("i")
        .and_then(|k| k.join(&file.hash_prefix()))
        .and_then(|k| k.join(&format!("{file}.idx")))
        .unwrap_or_else(|e| unreachable!("layout keys are built from validated parts: {e}"))
}

#[cfg(test)]
mod tests {
    use tessera_types::{Kind, Timestamp, Window};

    use super::*;

    fn partition_id() -> PartitionId {
        PartitionId {
            tenant: TenantId::new("acme").unwrap(),
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: Window::containing(Timestamp::UNIX_EPOCH, Window::DEFAULT_LENGTH).unwrap(),
        }
    }

    #[test]
    fn files_indexes_and_log_entries_never_change() {
        let file = FileId::from_u128(7);
        let writer = WriterId::from_u128(9);
        assert!(is_immutable(&data_file(&partition_id(), file)));
        assert!(is_immutable(&sidecar(&partition_id(), file)));
        assert!(is_immutable(&writer_log_entry(writer, Seq::new(3))));
        for key in [catalog(), leases(), node(NodeId::from_u128(1))] {
            assert!(!is_immutable(&key), "{key}");
        }
        let hint = catalog().join("_hint").unwrap();
        assert!(!is_immutable(&hint));
    }

    #[test]
    fn cluster_keys_follow_the_spec() {
        assert_eq!(catalog().as_str(), "_cluster/catalog");
        assert_eq!(leases().as_str(), "_cluster/leases");
        assert_eq!(
            node(NodeId::from_u128(1)).as_str(),
            "_cluster/nodes/00000000000000000000000000000001"
        );
        assert_eq!(
            probe(NodeId::from_u128(1)).as_str(),
            "_cluster/probes/00000000000000000000000000000001"
        );
    }

    #[test]
    fn writer_log_keys_follow_the_spec() {
        let writer = WriterId::from_u128(0xab);
        assert_eq!(
            writer_log(writer).as_str(),
            "_wal/000000000000000000000000000000ab"
        );
        assert_eq!(
            writer_log_entry(writer, Seq::new(7)).as_str(),
            "_wal/000000000000000000000000000000ab/000000000007"
        );
    }

    #[test]
    fn rule_group_keys_keep_the_partials_of_a_partition_count_together() {
        let tenant = TenantId::new("acme").unwrap();
        let writer = WriterId::from_u128(0xab);
        assert_eq!(eval_log(&tenant, "g", 1).as_str(), "acme/_eval/g/1");
        assert_eq!(
            eval_split(&tenant, "g", "2-7").as_str(),
            "acme/_eval/g/_split/2-7"
        );
        let all = eval_partials_of(&tenant, "g", 2);
        let of_writer = eval_partials(&tenant, "g", 2, 1, writer);
        let one = eval_partial(&tenant, "g", 2, 1, writer, Seq::new(7));
        assert_eq!(all.as_str(), "acme/_eval/g/_partials/2");
        assert_eq!(
            one.as_str(),
            "acme/_eval/g/_partials/2/1/000000000000000000000000000000ab/000000000007"
        );
        assert!(one.is_under(&of_writer) && of_writer.is_under(&all));
        assert!(!one.is_under(&eval_partials_of(&tenant, "g", 3)));
    }

    #[test]
    fn tenant_and_dataset_keys_follow_the_spec() {
        let tenant = TenantId::new("acme").unwrap();
        let dataset = DatasetName::new("prod").unwrap();
        assert_eq!(control_log(&tenant).as_str(), "acme/_control");
        assert_eq!(dataset_log(&tenant, &dataset).as_str(), "acme/prod");
        assert_eq!(
            writer_registry(&tenant, &dataset).as_str(),
            "acme/prod/_writers"
        );
        assert_eq!(
            writer_registration(&tenant, &dataset, WriterId::from_u128(1)).as_str(),
            "acme/prod/_writers/00000000000000000000000000000001"
        );
        assert_eq!(
            partitions(&tenant, &dataset, Kind::Span).as_str(),
            "acme/prod/p/span"
        );
    }

    #[test]
    fn partition_and_file_keys_follow_the_spec() {
        let id = partition_id();
        assert_eq!(
            partition(&id).as_str(),
            "acme/prod/p/log/000000000000-0086400"
        );
        let file = FileId::from_u128(0xab00_0000_0000_0000_0000_0000_0000_0001);
        assert_eq!(
            data_file(&id, file).as_str(),
            "acme/prod/p/log/000000000000-0086400/d/ab/ab000000000000000000000000000001.parquet"
        );
        assert_eq!(
            sidecar(&id, file).as_str(),
            "acme/prod/p/log/000000000000-0086400/i/ab/ab000000000000000000000000000001.idx"
        );
    }

    #[test]
    fn dataset_names_cannot_collide_with_tenant_prefixes() {
        assert!(DatasetName::new("_control").is_err());
    }
}
