// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! `lore_storage_mark_durable` sets `PayloadStoredDurable` on local entries without uploading,
//! walks fragment lists leaves-first, and creates nothing for content the store does not hold.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use lore::call::setup_execution;
use lore::storage::flush::LoreStorageFlushArgs;
use lore::storage::handle;
use lore::storage::handle::LoreStore;
use lore::storage::mark_durable::LoreStorageMarkDurableArgs;
use lore::storage::mark_durable::LoreStorageMarkDurableItem;
use lore::storage::open::LoreStorageOpenArgs;
use lore::storage::put::LoreStoragePutArgs;
use lore::storage::put::LoreStoragePutItem;
use lore::storage::store::disk_backed_for_tests;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;
use lore_base::types::FragmentReference;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_base::types::TypedBytes;
use lore_revision::event::LoreBytes;
use lore_revision::event::LoreErrorCode;
use lore_revision::event::LoreEvent;
use lore_revision::interface::LoreArray;
use lore_revision::interface::LoreEventCallback;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::store::event::LoreStorageMarkDurableItemCompleteEventData;
use lore_storage::options::ReadOptions;
use lore_storage::read::load_fragment;
use lore_storage::store_types::StoreMatch;
use tokio::sync::Semaphore;

fn partition() -> Partition {
    Partition::from([0x4Du8; 16])
}
fn other_partition() -> Partition {
    Partition::from([0x4Eu8; 16])
}
fn context() -> Context {
    Context::from([0x44u8; 16])
}

/// Leaf size for the fragmented cases: small enough that a few hundred KiB makes several leaves.
const LEAF: usize = 64 * 1024;

async fn open_in_memory() -> LoreStore {
    let opened: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
    let sink = opened.clone();
    let callback: LoreEventCallback = Some(Box::new(move |event: &LoreEvent| {
        if let LoreEvent::StorageOpened(data) = event {
            *sink.lock().unwrap() = Some(data.handle_id);
        }
    }));
    let status = lore::storage::open::open(
        LoreGlobalArgs::default(),
        LoreStorageOpenArgs {
            in_memory: 1,
            ..Default::default()
        },
        callback,
    )
    .await;
    assert_eq!(status, 0, "opening an in-memory store must succeed");
    LoreStore {
        handle_id: opened.lock().unwrap().expect("open must report a handle"),
    }
}

/// Put `payload` locally only, cut into `fixed_size_chunk` leaves when non-zero.
async fn put(handle: LoreStore, payload: &[u8], fixed_size_chunk: u64) -> Address {
    let address: Arc<Mutex<Option<Address>>> = Arc::new(Mutex::new(None));
    let sink = address.clone();
    let callback: LoreEventCallback = Some(Box::new(move |event: &LoreEvent| {
        if let LoreEvent::StoragePutItemComplete(data) = event {
            assert_eq!(
                data.error.error_code,
                LoreErrorCode::None as i32,
                "put item must succeed: {:?}",
                data.error.message
            );
            *sink.lock().unwrap() = Some(data.address);
        }
    }));
    let status = lore::storage::put::put(
        LoreGlobalArgs::default(),
        LoreStoragePutArgs {
            handle,
            items: LoreArray::from_vec(vec![LoreStoragePutItem {
                id: 1,
                partition: partition(),
                context: context(),
                data: LoreBytes {
                    ptr: payload.as_ptr().cast(),
                    len: payload.len(),
                },
                remote_write: 0,
                local_cache: 0,
                fixed_size_chunk,
            }]),
        },
        callback,
    )
    .await;
    assert_eq!(status, 0, "put must succeed");
    address.lock().unwrap().expect("put must report an address")
}

async fn mark(
    handle: LoreStore,
    items: Vec<LoreStorageMarkDurableItem>,
) -> (i32, Vec<LoreStorageMarkDurableItemCompleteEventData>) {
    mark_with(LoreGlobalArgs::default(), handle, items).await
}

async fn mark_with(
    globals: LoreGlobalArgs,
    handle: LoreStore,
    items: Vec<LoreStorageMarkDurableItem>,
) -> (i32, Vec<LoreStorageMarkDurableItemCompleteEventData>) {
    let completes: Arc<Mutex<Vec<LoreStorageMarkDurableItemCompleteEventData>>> =
        Arc::new(Mutex::new(Vec::new()));
    let sink = completes.clone();
    let callback: LoreEventCallback = Some(Box::new(move |event: &LoreEvent| {
        if let LoreEvent::StorageMarkDurableItemComplete(data) = event {
            sink.lock().unwrap().push(data.clone());
        }
    }));
    let status = lore::storage::mark_durable::mark_durable(
        globals,
        LoreStorageMarkDurableArgs {
            handle,
            items: LoreArray::from_vec(items),
        },
        callback,
    )
    .await;
    let completes = completes.lock().unwrap().clone();
    (status, completes)
}

async fn mark_one(
    handle: LoreStore,
    address: Address,
) -> (i32, LoreStorageMarkDurableItemCompleteEventData) {
    mark_one_in(handle, partition(), address).await
}

async fn mark_one_in(
    handle: LoreStore,
    partition: Partition,
    address: Address,
) -> (i32, LoreStorageMarkDurableItemCompleteEventData) {
    let (status, completes) = mark(
        handle,
        vec![LoreStorageMarkDurableItem {
            id: 7,
            partition,
            address,
        }],
    )
    .await;
    assert_eq!(completes.len(), 1, "one item must emit one completion");
    assert_eq!(completes[0].id, 7);
    (status, completes[0].clone())
}

/// The local store's own record for `address`, read past the C API.
async fn local_entry(handle: LoreStore, address: Address) -> Option<Fragment> {
    local_entry_in(handle, partition(), address).await
}

async fn local_entry_in(
    handle: LoreStore,
    partition: Partition,
    address: Address,
) -> Option<Fragment> {
    let store = handle::immutable_for_test(handle).expect("handle is open");
    let data = store.get_metadata(partition, address).await.ok()?;
    (data.match_made == StoreMatch::MatchFull).then_some(data.fragment)
}

fn is_durable(fragment: Fragment) -> bool {
    fragment.flags & FragmentFlags::PayloadStoredDurable.bits() != 0
}

fn is_local(fragment: Fragment) -> bool {
    fragment.flags & FragmentFlags::PayloadStoredLocal.bits() != 0
}

fn is_list(fragment: Fragment) -> bool {
    fragment.flags & FragmentFlags::PayloadFragmented.bits() != 0
}

/// Rewrite `address`'s local entry with `PayloadStoredDurable` set, as `upload` leaves the root
/// of what it uploads.
async fn set_durable(handle: LoreStore, address: Address) {
    let mut fragment = local_entry(handle, address).await.expect("entry is stored");
    fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
    handle::immutable_for_test(handle)
        .unwrap()
        .put(partition(), address, fragment, None, false)
        .await
        .expect("rewriting the flags must succeed");
}

/// The addresses a fragment list's local payload references.
async fn list_children(handle: LoreStore, address: Address) -> Vec<Address> {
    let store = handle::immutable_for_test(handle).unwrap();
    let (_, list) = load_fragment(
        store,
        partition(),
        address,
        ReadOptions::default().no_remote(),
        None,
    )
    .await
    .expect("the list is stored locally");
    let list = list.to_aligned::<FragmentReference>();
    list.as_type_slice::<FragmentReference>()
        .iter()
        .map(|reference| Address {
            hash: reference.hash,
            context: address.context,
        })
        .collect()
}

/// Bytes whose `LEAF`-sized chunks all differ, so every leaf is its own entry. Content up to
/// 256 KiB is stored as one fragment, so fragmented cases need more than four leaves.
fn distinct_leaves(leaves: usize) -> Vec<u8> {
    distinct_bytes(leaves * LEAF)
}

/// Pseudo-random bytes, so chunks of a few dozen bytes or more do not repeat.
fn distinct_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn leaf_addresses(payload: &[u8]) -> Vec<Address> {
    chunk_addresses(payload, LEAF)
}

fn chunk_addresses(payload: &[u8], chunk: usize) -> Vec<Address> {
    payload
        .chunks(chunk)
        .map(|chunk| Address {
            hash: lore_storage::hash::hash_slice(chunk),
            context: context(),
        })
        .collect()
}

#[tokio::test]
async fn marks_a_single_fragment_and_keeps_its_payload() {
    let handle = open_in_memory().await;
    let address = put(handle, b"content made durable by another store", 0).await;
    let before = local_entry(handle, address).await.expect("put stored it");
    assert!(!is_durable(before), "a local-only put is not durable");

    let (status, complete) = mark_one(handle, address).await;
    assert_eq!(status, 0);
    assert_eq!(complete.error.error_code, LoreErrorCode::None as i32);
    assert_eq!(complete.address, address);
    assert_eq!(complete.already_durable, 0);
    assert_eq!(complete.marked, 1);

    let after = local_entry(handle, address).await.expect("entry is kept");
    assert!(is_durable(after), "the entry must be durable after marking");
    assert!(is_local(after), "marking must leave the payload in place");
    assert_eq!(after.size_content, before.size_content);
}

#[tokio::test]
async fn marking_twice_reports_already_durable() {
    let handle = open_in_memory().await;
    let address = put(handle, b"marked twice", 0).await;
    assert_eq!(mark_one(handle, address).await.1.marked, 1);

    let (status, complete) = mark_one(handle, address).await;
    assert_eq!(status, 0);
    assert_eq!(complete.error.error_code, LoreErrorCode::None as i32);
    assert_eq!(complete.already_durable, 1);
    assert_eq!(complete.marked, 0);
}

#[tokio::test]
async fn marks_every_leaf_of_fragmented_content() {
    let handle = open_in_memory().await;
    let payload = distinct_leaves(6);
    let root = put(handle, &payload, LEAF as u64).await;
    let leaves = leaf_addresses(&payload);
    assert_eq!(leaves.len(), 6);
    let root_entry = local_entry(handle, root).await.expect("root is stored");
    assert!(
        is_list(root_entry),
        "the content must have been stored as a fragment list"
    );
    for leaf in &leaves {
        let entry = local_entry(handle, *leaf).await.expect("leaf is stored");
        assert!(!is_durable(entry));
    }

    let (status, complete) = mark_one(handle, root).await;
    assert_eq!(status, 0);
    assert_eq!(complete.error.error_code, LoreErrorCode::None as i32);
    assert_eq!(complete.marked, 1 + leaves.len() as u64);

    assert!(is_durable(local_entry(handle, root).await.unwrap()));
    for leaf in &leaves {
        let entry = local_entry(handle, *leaf).await.unwrap();
        assert!(is_durable(entry), "leaf {leaf} must be durable");
        assert!(is_local(entry), "leaf {leaf} must keep its payload");
    }

    let (_, again) = mark_one(handle, root).await;
    assert_eq!(again.already_durable, 1);
    assert_eq!(again.marked, 0);
}

/// `upload` marks only the root of fragmented content, so a durable root says nothing about the
/// leaves below it: they are still walked and marked, and the item is not `already_durable`.
#[tokio::test]
async fn marks_the_leaves_below_a_root_already_durable() {
    let handle = open_in_memory().await;
    let payload = distinct_leaves(6);
    let root = put(handle, &payload, LEAF as u64).await;
    let leaves = leaf_addresses(&payload);
    set_durable(handle, root).await;

    let (status, complete) = mark_one(handle, root).await;
    assert_eq!(status, 0);
    assert_eq!(complete.error.error_code, LoreErrorCode::None as i32);
    assert_eq!(complete.already_durable, 0);
    assert_eq!(complete.marked, leaves.len() as u64);
    for leaf in &leaves {
        assert!(is_durable(local_entry(handle, *leaf).await.unwrap()));
    }
}

/// Content with more leaves than one list holds is a list of lists; every level is marked. Leaves
/// must be larger than a list reference, or a full sublist would outweigh the content it lists.
#[tokio::test]
async fn marks_every_level_of_a_nested_fragment_list() {
    const CHUNK: usize = 64;
    let handle = open_in_memory().await;
    let payload = distinct_bytes(512 * 1024);
    let root = put(handle, &payload, CHUNK as u64).await;
    let leaves = chunk_addresses(&payload, CHUNK);
    let sublists = list_children(handle, root).await;
    assert!(sublists.len() > 1, "the root must list several sublists");
    for sublist in &sublists {
        let entry = local_entry(handle, *sublist)
            .await
            .expect("sublist is stored");
        assert!(
            is_list(entry),
            "the root's children must be lists themselves"
        );
    }

    let (status, complete) = mark_one(handle, root).await;
    assert_eq!(status, 0);
    assert_eq!(complete.error.error_code, LoreErrorCode::None as i32);
    assert_eq!(
        complete.marked,
        1 + sublists.len() as u64 + leaves.len() as u64
    );
    for address in sublists.iter().chain(&leaves) {
        assert!(is_durable(local_entry(handle, *address).await.unwrap()));
    }
}

#[tokio::test]
async fn a_missing_leaf_fails_the_item_and_leaves_the_root_unmarked() {
    let handle = open_in_memory().await;
    let payload = distinct_leaves(6);
    let root = put(handle, &payload, LEAF as u64).await;
    let leaves = leaf_addresses(&payload);

    let store = handle::immutable_for_test(handle).unwrap();
    store
        .clone()
        .obliterate(partition(), leaves[2], Default::default())
        .await
        .expect("obliterating a leaf must succeed");
    assert!(local_entry(handle, leaves[2]).await.is_none());

    let (status, complete) = mark_one(handle, root).await;
    assert_ne!(status, 0);
    assert_eq!(
        complete.error.error_code,
        LoreErrorCode::AddressNotFound as i32
    );
    assert!(
        !is_durable(local_entry(handle, root).await.unwrap()),
        "the root must stay unmarked so a retry walks the list again"
    );
    let mut durable_leaves = 0;
    for leaf in &leaves {
        if let Some(entry) = local_entry(handle, *leaf).await
            && is_durable(entry)
        {
            durable_leaves += 1;
        }
    }
    assert!(
        durable_leaves > 0,
        "leaves walked before the miss stay marked"
    );
    assert_eq!(
        complete.marked, durable_leaves,
        "`marked` counts the leaves marked before the miss"
    );
}

#[tokio::test]
async fn a_missing_address_is_not_found_and_creates_nothing() {
    let handle = open_in_memory().await;
    let address = Address {
        hash: lore_storage::hash::hash_slice(b"never stored"),
        context: context(),
    };

    let (status, complete) = mark_one(handle, address).await;
    assert_ne!(status, 0);
    assert_eq!(
        complete.error.error_code,
        LoreErrorCode::AddressNotFound as i32
    );
    assert_eq!(complete.address, Address::default());
    assert!(local_entry(handle, address).await.is_none());
}

/// An entry that records content without holding its payload cannot be marked: marking it would
/// claim a payload this store does not hold.
#[tokio::test]
async fn an_entry_without_a_local_payload_is_not_found() {
    let handle = open_in_memory().await;
    let payload = b"described here, held elsewhere";
    let address = Address {
        hash: lore_storage::hash::hash_slice(payload),
        context: context(),
    };
    let fragment = Fragment {
        flags: 0,
        size_payload: payload.len() as u32,
        size_content: payload.len() as u64,
    };
    handle::immutable_for_test(handle)
        .unwrap()
        .put(partition(), address, fragment, None, false)
        .await
        .expect("a metadata-only put must succeed");
    let entry = local_entry(handle, address)
        .await
        .expect("metadata is stored");
    assert!(!is_local(entry));

    let (status, complete) = mark_one(handle, address).await;
    assert_ne!(status, 0);
    assert_eq!(
        complete.error.error_code,
        LoreErrorCode::AddressNotFound as i32
    );
    assert!(!is_durable(local_entry(handle, address).await.unwrap()));
}

/// Content stored under one partition is not found under another, and nothing is created there.
#[tokio::test]
async fn content_in_another_partition_is_not_found() {
    let handle = open_in_memory().await;
    let address = put(handle, b"stored in the first partition", 0).await;

    let (status, complete) = mark_one_in(handle, other_partition(), address).await;
    assert_ne!(status, 0);
    assert_eq!(
        complete.error.error_code,
        LoreErrorCode::AddressNotFound as i32
    );
    assert!(
        local_entry_in(handle, other_partition(), address)
            .await
            .is_none()
    );
    assert!(!is_durable(local_entry(handle, address).await.unwrap()));
}

#[tokio::test]
async fn a_zero_hash_succeeds_and_a_zero_partition_is_rejected() {
    let handle = open_in_memory().await;
    let (status, completes) = mark(
        handle,
        vec![
            LoreStorageMarkDurableItem {
                id: 1,
                partition: partition(),
                address: Address {
                    hash: Hash::default(),
                    context: context(),
                },
            },
            LoreStorageMarkDurableItem {
                id: 2,
                partition: Partition::default(),
                address: Address::default(),
            },
        ],
    )
    .await;
    assert_ne!(status, 0);
    let by_id = |id| completes.iter().find(|c| c.id == id).cloned().unwrap();
    assert_eq!(by_id(1).error.error_code, LoreErrorCode::None as i32);
    assert_eq!(by_id(1).already_durable, 1);
    assert_eq!(
        by_id(2).error.error_code,
        LoreErrorCode::InvalidArguments as i32
    );
}

/// A call that bypasses the local store has no entry to mark and is rejected whole.
#[tokio::test]
async fn bypassing_the_local_store_is_rejected() {
    let handle = open_in_memory().await;
    let address = put(handle, b"only held locally", 0).await;

    let (status, completes) = mark_with(
        LoreGlobalArgs {
            remote: 1,
            ..LoreGlobalArgs::default()
        },
        handle,
        vec![LoreStorageMarkDurableItem {
            id: 1,
            partition: partition(),
            address,
        }],
    )
    .await;
    assert_eq!(status, LoreErrorCode::InvalidArguments as i32);
    assert!(completes.is_empty(), "a call rejected whole emits no item");
    assert!(!is_durable(local_entry(handle, address).await.unwrap()));
}

/// The point of marking: eviction reclaims marked entries and still protects the rest.
#[tokio::test]
async fn eviction_reclaims_only_marked_entries() {
    let handle = open_in_memory().await;
    let kept = put(handle, b"still only held here", 0).await;
    let marked = put(handle, b"held durably elsewhere", 0).await;
    assert_eq!(mark_one(handle, marked).await.1.marked, 1);

    let store = handle::immutable_for_test(handle).unwrap();
    store
        .evict(0, false, None)
        .await
        .expect("evict must succeed");

    assert!(
        local_entry(handle, kept).await.is_some(),
        "an unmarked entry must survive eviction"
    );
    assert!(
        local_entry(handle, marked).await.is_none(),
        "a marked entry must be evictable"
    );
}

/// The mark is written to the store itself: a disk-backed store opened again still has it.
#[tokio::test]
async fn the_mark_survives_reopening_a_disk_backed_store() {
    let dir = lore_base::test_util::TempDir::new("mark-durable-reopen-");
    let first = handle::register(disk_backed_for_tests("mark-durable-reopen", dir.path()).await);
    let payload = distinct_leaves(6);
    let root = put(first, &payload, LEAF as u64).await;
    assert_eq!(mark_one(first, root).await.1.marked, 7);
    let status = lore::storage::flush::flush(
        LoreGlobalArgs::default(),
        LoreStorageFlushArgs { handle: first },
        None,
    )
    .await;
    assert_eq!(status, 0, "flushing must succeed");
    drop(handle::unregister(first).expect("the handle is registered"));

    let second = handle::register(disk_backed_for_tests("mark-durable-reopen", dir.path()).await);
    for address in std::iter::once(root).chain(leaf_addresses(&payload)) {
        let entry = local_entry(second, address).await.expect("entry is kept");
        assert!(is_durable(entry), "{address} must still be durable");
        assert!(is_local(entry), "{address} must still hold its payload");
    }
    drop(handle::unregister(second).expect("the handle is registered"));
}

/// Leaf size and payload length of the nested cases: more leaves than one list holds, so the
/// root lists sublists.
const NESTED_CHUNK: usize = 64;
const NESTED_LEN: usize = 512 * 1024;

/// Whether the local entry for `address` is durable; `false` when there is none.
async fn durable_entry(handle: LoreStore, address: Address) -> bool {
    local_entry(handle, address).await.is_some_and(is_durable)
}

/// A sublist missing locally fails the item before any list above it is marked: the walk marks a
/// list only once everything below it is.
#[tokio::test]
async fn a_missing_sublist_fails_the_item_and_leaves_the_lists_above_unmarked() {
    let handle = open_in_memory().await;
    let payload = distinct_bytes(NESTED_LEN);
    let root = put(handle, &payload, NESTED_CHUNK as u64).await;
    let leaves = chunk_addresses(&payload, NESTED_CHUNK);
    let sublists = list_children(handle, root).await;
    handle::immutable_for_test(handle)
        .unwrap()
        .obliterate(partition(), sublists[0], Default::default())
        .await
        .expect("obliterating a sublist must succeed");

    let (status, complete) = mark_one(handle, root).await;
    assert_ne!(status, 0);
    assert_eq!(
        complete.error.error_code,
        LoreErrorCode::AddressNotFound as i32
    );
    assert!(
        !durable_entry(handle, root).await,
        "the root must stay unmarked"
    );
    let mut durable = 0;
    for address in sublists.iter().chain(&leaves) {
        durable += u64::from(durable_entry(handle, *address).await);
    }
    assert_eq!(
        complete.marked, durable,
        "`marked` counts the entries marked before the miss"
    );
}

/// A call that failed part way leaves what it marked marked; once the missing entry is stored
/// again, a retry marks the rest, and the two calls together mark every entry once.
#[tokio::test]
async fn a_retry_marks_what_a_failed_call_left() {
    let handle = open_in_memory().await;
    let payload = distinct_leaves(6);
    let root = put(handle, &payload, LEAF as u64).await;
    let leaves = leaf_addresses(&payload);
    handle::immutable_for_test(handle)
        .unwrap()
        .obliterate(partition(), leaves[2], Default::default())
        .await
        .expect("obliterating a leaf must succeed");
    let (_, failed) = mark_one(handle, root).await;
    assert_ne!(failed.error.error_code, LoreErrorCode::None as i32);

    assert_eq!(put(handle, &payload, LEAF as u64).await, root);
    let (status, retried) = mark_one(handle, root).await;
    assert_eq!(status, 0);
    assert_eq!(retried.error.error_code, LoreErrorCode::None as i32);
    assert_eq!(retried.already_durable, 0);
    assert_eq!(
        failed.marked + retried.marked,
        1 + leaves.len() as u64,
        "the root and every leaf are each marked once across the two calls"
    );
    for address in std::iter::once(root).chain(leaves) {
        assert!(
            durable_entry(handle, address).await,
            "{address} must be durable"
        );
    }
}

/// A durable list whose payload is not held here cannot be walked: the item succeeds without
/// visiting what is below it.
#[tokio::test]
async fn a_durable_list_without_its_payload_is_skipped() {
    let handle = open_in_memory().await;
    let address = Address {
        hash: lore_storage::hash::hash_slice(b"a list held only elsewhere"),
        context: context(),
    };
    let references = 2;
    let fragment = Fragment {
        flags: FragmentFlags::PayloadFragmented.bits() | FragmentFlags::PayloadStoredDurable.bits(),
        size_payload: (references * size_of::<FragmentReference>()) as u32,
        size_content: (references * LEAF) as u64,
    };
    handle::immutable_for_test(handle)
        .unwrap()
        .put(partition(), address, fragment, None, false)
        .await
        .expect("a metadata-only put must succeed");
    let entry = local_entry(handle, address)
        .await
        .expect("metadata is stored");
    assert!(is_durable(entry) && is_list(entry) && !is_local(entry));

    let (status, complete) = mark_one(handle, address).await;
    assert_eq!(status, 0);
    assert_eq!(complete.error.error_code, LoreErrorCode::None as i32);
    assert_eq!(complete.already_durable, 1);
    assert_eq!(complete.marked, 0);
}

/// The items of a call share its permits, so the entries open at once are bounded per call, not
/// per item: two items whose lists each reference thousands of entries complete under a single
/// permit, and every permit is returned.
#[tokio::test]
async fn the_items_of_a_call_share_its_permits() {
    let handle = open_in_memory().await;
    let first = distinct_bytes(NESTED_LEN);
    let second: Vec<u8> = first.iter().rev().copied().collect();
    let roots = [
        put(handle, &first, NESTED_CHUNK as u64).await,
        put(handle, &second, NESTED_CHUNK as u64).await,
    ];
    let items: Vec<_> = roots
        .iter()
        .zip(1..)
        .map(|(address, id)| LoreStorageMarkDurableItem {
            id,
            partition: partition(),
            address: *address,
        })
        .collect();

    let permits = Arc::new(Semaphore::new(1));
    let store = handle::immutable_for_test(handle).unwrap();
    let marking = LORE_CONTEXT.scope(
        setup_execution(LoreGlobalArgs::default(), None),
        lore::storage::mark_durable::mark_items(store, &items, permits.clone()),
    );
    tokio::time::timeout(Duration::from_secs(60), marking)
        .await
        .expect("items sharing one permit must not wait on each other forever")
        .expect("marking must succeed");

    assert_eq!(permits.available_permits(), 1, "every permit is returned");
    for address in roots
        .into_iter()
        .chain(chunk_addresses(&first, NESTED_CHUNK))
        .chain(chunk_addresses(&second, NESTED_CHUNK))
    {
        assert!(
            durable_entry(handle, address).await,
            "{address} must be durable"
        );
    }
}
