// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! `lore_storage_mark_durable` — record that locally-stored content is durable elsewhere.
//!
//! The caller asserts that the content is held durably outside this store. Nothing is uploaded
//! and no remote is consulted: the local entry gains `PayloadStoredDurable`, which is what lets
//! eviction reclaim its payload and what writes, `upload` and push read as "already durable".
//!
//! The flag is kept per partition and names no remote. If this store is also used with a remote
//! under the same partition, only content that remote holds too may be marked: writes, `upload`
//! and push to it skip a marked entry, so it would never receive the content.
//!
//! Whole-call pre-dispatch rejects the call when local access is bypassed (`globals.remote=1`
//! on the handle or the call), since there is no local entry to mark.
//!
//! Per-item:
//! - `partition == Partition::default()` → `INVALID_ARGUMENTS`.
//! - `address.hash == Hash::default()` → no-op success with `already_durable=1`.
//! - No full local match for the address, or a match without a local payload →
//!   `ADDRESS_NOT_FOUND`. No entry is created.
//! - A fragmented entry is walked first: every leaf and nested list below it is marked before
//!   the entry itself, so a call that fails part way leaves the root unmarked and a retry walks
//!   the list again. A list already durable is still walked, since `upload` marks only the root
//!   of what it uploads; one whose payload is not held here cannot be, and is skipped without
//!   visiting the entries below it. A non-durable list or a leaf missing locally fails the item
//!   with `ADDRESS_NOT_FOUND`, or `PAYLOAD_NOT_FOUND` when it is obliterated while being read.
//! - Otherwise the entry's flags are rewritten with `PayloadStoredDurable` set and the payload
//!   left where it is.
//!
//! The entries a list references are opened and marked concurrently, up to `MAX_PARALLEL_MARK`
//! at a time across the whole call. Lists are walked one after another, each loaded only when
//! the walk reaches it, so an item holds one list payload at a time.
//!
//! `marked` counts the puts an item made. An entry referenced more than once, by one item or by
//! items marked concurrently, can be counted once per reference.

use std::sync::Arc;

use bytes::Bytes;
use lore_base::error::AddressNotFound;
use lore_base::error::InvalidArguments;
use lore_base::lore_spawn;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;
use lore_base::types::FragmentReference;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_base::types::TypedBytes;
use lore_error_set::prelude::*;
use lore_macro::LoreArgs;
use lore_macro::ValidateText;
use lore_revision::event::LoreEvent;
use lore_revision::interface::LoreArray;
use lore_revision::store::event::LoreStorageMarkDurableItemCompleteEventData;
use lore_storage::StorageError;
use lore_storage::immutable_store::ImmutableStore;
use lore_storage::options::ReadOptions;
use lore_storage::read::load_fragment;
use lore_storage::store_types::StoreMatch;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::call_delegation::dispatch_call;
use crate::interface::LoreEventCallback;
use crate::interface::LoreGlobalArgs;
use crate::storage::call::storage_call;
use crate::storage::handle::LoreStore;
use crate::storage::invalid_item;
use crate::storage::item_detail;

/// One mark-durable item — the `(partition, address)` of locally-stored content to mark durable.
#[repr(C)]
#[derive(
    Copy, Clone, Default, Debug, PartialEq, ValidateText, bitcode::Encode, bitcode::Decode,
)]
pub struct LoreStorageMarkDurableItem {
    /// Caller-chosen id echoed back in `LORE_EVENT_STORAGE_MARK_DURABLE_ITEM_COMPLETE`
    pub id: u64,
    /// Partition of the local content; the zero partition rejects with `INVALID_ARGUMENTS`
    pub partition: Partition,
    /// Local content address to mark; a zero hash is no-op success with `already_durable=1`
    pub address: Address,
}

/// Arguments for `lore_storage_mark_durable`.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default, LoreArgs, bitcode::Encode, bitcode::Decode)]
#[handler(mark_durable_local)]
pub struct LoreStorageMarkDurableArgs {
    /// Open storage handle; needs no remote
    pub handle: LoreStore,
    /// Addresses to mark; each runs independently and emits its own
    /// `LORE_EVENT_STORAGE_MARK_DURABLE_ITEM_COMPLETE`
    pub items: LoreArray<LoreStorageMarkDurableItem>,
}

/// Mark one or more `(partition, address)` entries of the local store durable.
pub async fn mark_durable(
    globals: LoreGlobalArgs,
    args: LoreStorageMarkDurableArgs,
    callback: LoreEventCallback,
) -> i32 {
    dispatch_call(globals, args, callback, mark_durable_local).await
}

async fn mark_durable_local(
    globals: LoreGlobalArgs,
    args: LoreStorageMarkDurableArgs,
    callback: LoreEventCallback,
) -> i32 {
    let handle = args.handle;
    let per_call = crate::storage::store::PerCallFlags::from_globals(&globals);
    storage_call(
        globals,
        callback,
        handle,
        args,
        mark_durable,
        async move |store, args| {
            let effective = store.effective_flags(per_call)?;
            if effective.no_local {
                return Err(StorageError::from(InvalidArguments {
                    reason: "mark_durable needs the local store; incompatible with the `remote` \
                             flag set on handle or call"
                        .into(),
                }));
            }

            let permits = Arc::new(Semaphore::new(MAX_PARALLEL_MARK));
            mark_items(store.immutable.clone(), args.items.as_slice(), permits).await
        },
    )
    .await
}

/// Entries one call opens and marks at once.
const MAX_PARALLEL_MARK: usize = 1000;

/// Mark every item, each emitting its own completion. The entries the items' lists reference
/// share `permits`, one per entry being opened or marked.
#[lore_macro::test_pub]
pub(crate) async fn mark_items(
    store: Arc<dyn ImmutableStore>,
    items: &[LoreStorageMarkDurableItem],
    permits: Arc<Semaphore>,
) -> Result<(), StorageError> {
    if items.is_empty() {
        return Ok(());
    }
    crate::storage::fan_out_items!(items, "mark_durable", |item| {
        mark_durable_item(store.clone(), &item, permits.clone())
    })
}

/// Not an `async fn`, which would hold a second copy of its arguments.
#[allow(clippy::manual_async_fn)]
fn mark_durable_item(
    store: Arc<dyn ImmutableStore>,
    item: &LoreStorageMarkDurableItem,
    permits: Arc<Semaphore>,
) -> impl Future<Output = Result<(), StorageError>> + Send + 'static {
    let item = *item;
    async move {
        if item.partition == Partition::default() {
            return emit_complete(
                &item,
                0,
                0,
                Err(invalid_item("item names the default partition")),
            );
        }
        if item.address.hash == Hash::default() {
            return emit_complete(&item, 1, 0, Ok(()));
        }

        let mut marked = 0;
        let result = mark_tree(&store, &permits, item.partition, item.address, &mut marked).await;
        let already_durable = u8::from(result.is_ok() && marked == 0);
        emit_complete(&item, already_durable, marked, result)
    }
}

/// Mark `address` durable, after every entry its fragment list references if it has one.
///
/// Children go first so a list this call marks is marked only once everything below it is,
/// except below a durable list skipped because its payload is not held here. A list already
/// durable is walked too, since `upload` marks the root it uploads and not the leaves below it.
/// `marked` counts the puts made, including those made before a failure.
///
/// Not an `async fn`, which would hold a second copy of its arguments.
#[allow(clippy::manual_async_fn)]
fn mark_tree<'a>(
    store: &'a Arc<dyn ImmutableStore>,
    permits: &'a Arc<Semaphore>,
    partition: Partition,
    address: Address,
    marked: &'a mut u64,
) -> impl Future<Output = Result<(), StorageError>> + Send + 'a {
    // Iterative depth-first walk over lists, post-order: a list's `Mark` step is pushed beneath
    // the `List` steps of its sublists, so it runs only once its whole subtree is marked. The
    // root is taken first, so an item that is not a list never allocates the stack.
    let mut next = Some(Step::Open(address));
    async move {
        let mut stack = Vec::new();
        loop {
            let Some(step) = next.take().or_else(|| stack.pop()) else {
                return Ok(());
            };
            let (address, unmarked) = match step {
                Step::Mark(address, fragment) => {
                    mark_entry(store, partition, address, fragment).await?;
                    *marked += 1;
                    continue;
                }
                Step::List(address, unmarked) => (address, unmarked),
                Step::Open(address) => match open_entry(store, partition, address).await? {
                    Entry::Done => continue,
                    Entry::Leaf(fragment) => {
                        mark_entry(store, partition, address, fragment).await?;
                        *marked += 1;
                        continue;
                    }
                    Entry::List(unmarked) => (address, unmarked),
                },
            };
            let Some(list) = load_list(store, partition, address, unmarked.is_none()).await? else {
                continue;
            };
            if let Some(fragment) = unmarked {
                stack.push(Step::Mark(address, fragment));
            }
            mark_children(
                store,
                permits,
                partition,
                address.context,
                &list,
                &mut stack,
                marked,
            )
            .await?;
        }
    }
}

/// Work left in [`mark_tree`]'s walk.
enum Step {
    /// Look up the item's own entry, and walk it if it is a list.
    Open(Address),
    /// Walk a list found below the item, with its fragment to mark afterwards when it is not
    /// durable yet.
    List(Address, Option<Fragment>),
    /// Mark a list whose subtree is marked.
    Mark(Address, Fragment),
}

/// What [`open_and_mark`] did with one entry.
enum Opened {
    /// The entry gained `PayloadStoredDurable`.
    Marked,
    /// The entry needed nothing.
    Done,
    /// The entry is a list, still to walk, with its fragment when it is not durable yet.
    List(Option<Fragment>),
}

/// Open and mark every entry `list` references, each holding one of `permits` while it runs,
/// pushing the sublists among them onto `stack`.
///
/// After a failure no further entry is started, but those in flight finish and are counted. The
/// first failure is returned.
///
/// Not an `async fn`, which would hold a second copy of its arguments.
#[allow(clippy::manual_async_fn)]
fn mark_children<'a>(
    store: &'a Arc<dyn ImmutableStore>,
    permits: &'a Arc<Semaphore>,
    partition: Partition,
    context: Context,
    list: &'a Bytes,
    stack: &'a mut Vec<Step>,
    marked: &'a mut u64,
) -> impl Future<Output = Result<(), StorageError>> + Send + 'a {
    async move {
        let mut references = list.as_type_slice::<FragmentReference>().iter().peekable();
        let mut tasks = JoinSet::new();
        let mut failure = None;
        loop {
            while failure.is_none() && references.peek().is_some() {
                // With nothing of its own in flight the walk waits for a permit; otherwise it
                // joins what it started, which lets its permits go.
                let permit = if tasks.is_empty() {
                    permits.clone().acquire_owned().await.map_err(|err| {
                        StorageError::internal_with_context(err, "acquiring a mark permit")
                    })?
                } else {
                    match permits.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => break,
                    }
                };
                let Some(reference) = references.next() else {
                    break;
                };
                let address = Address {
                    hash: reference.hash,
                    context,
                };
                lore_spawn!(
                    tasks,
                    open_and_mark(store.clone(), partition, address, permit)
                );
            }
            let Some(joined) = tasks.join_next().await else {
                return failure.map_or(Ok(()), Err);
            };
            let opened = joined.unwrap_or_else(|err| {
                Err(StorageError::internal_with_context(
                    err,
                    "joining a mark-durable task",
                ))
            });
            match opened {
                Ok((_, Opened::Marked)) => *marked += 1,
                Ok((_, Opened::Done)) => {}
                Ok((address, Opened::List(unmarked))) => stack.push(Step::List(address, unmarked)),
                Err(err) => {
                    failure.get_or_insert(err);
                }
            }
        }
    }
}

/// Open `address`'s local entry and mark it if it is a leaf, holding `permit` until done. A list
/// is only reported, to be loaded when the walk reaches it.
///
/// Not an `async fn`, which would hold a second copy of its arguments.
#[allow(clippy::manual_async_fn)]
fn open_and_mark(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> impl Future<Output = Result<(Address, Opened), StorageError>> + Send + 'static {
    async move {
        let opened = match open_entry(&store, partition, address).await? {
            Entry::Done => Opened::Done,
            Entry::Leaf(fragment) => {
                mark_entry(&store, partition, address, fragment).await?;
                Opened::Marked
            }
            Entry::List(unmarked) => Opened::List(unmarked),
        };
        drop(permit);
        Ok((address, opened))
    }
}

enum Entry {
    /// A durable leaf, or a durable list whose payload is not held here to walk.
    Done,
    /// A non-durable entry whose payload is content, with the fragment to write back.
    Leaf(Fragment),
    /// A fragment list, with the fragment to write back when the list itself is not durable yet.
    List(Option<Fragment>),
}

/// Look up the local entry for `address`.
///
/// Not an `async fn`, which would hold a second copy of its arguments.
#[allow(clippy::manual_async_fn)]
fn open_entry(
    store: &Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
) -> impl Future<Output = Result<Entry, StorageError>> + Send + '_ {
    async move {
        let metadata = store
            .clone()
            .get_metadata(partition, address)
            .await
            .forward::<StorageError>("reading the local metadata")?;
        if metadata.match_made != StoreMatch::MatchFull {
            return Err(StorageError::from(AddressNotFound::from(address)));
        }
        let fragment = metadata.fragment;
        let durable = fragment.flags & FragmentFlags::PayloadStoredDurable.bits() != 0;
        let fragmented = fragment.flags & FragmentFlags::PayloadFragmented.bits() != 0;
        let local = fragment.flags & FragmentFlags::PayloadStoredLocal.bits() != 0;
        Ok(match (durable, fragmented, local) {
            (true, false, _) | (true, true, false) => Entry::Done,
            // Marking a metadata-only entry durable would claim a payload nothing here holds.
            (false, _, false) => return Err(StorageError::from(AddressNotFound::from(address))),
            (false, false, true) => Entry::Leaf(fragment),
            (_, true, true) => Entry::List((!durable).then_some(fragment)),
        })
    }
}

/// Load the references the list at `address` holds, from the local store only: content missing
/// here must fail the item, never be fetched to satisfy it. A durable list whose payload is gone
/// by now, as eviction may take it after its metadata was read, is `None`, to be skipped. The
/// read is strict, so a failed or corrupt read fails the item with its own error rather than
/// passing for a miss.
///
/// Not an `async fn`, which would hold a second copy of its arguments.
#[allow(clippy::manual_async_fn)]
fn load_list(
    store: &Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    durable: bool,
) -> impl Future<Output = Result<Option<Bytes>, StorageError>> + Send + '_ {
    async move {
        match load_fragment(
            store.clone(),
            partition,
            address,
            ReadOptions::default().no_remote().with_strict_local(),
            None,
        )
        .await
        {
            Ok((_, list)) => Ok(Some(list.to_aligned::<FragmentReference>())),
            Err(err) if durable && (err.is_address_not_found() || err.is_payload_not_found()) => {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }
}

/// Rewrite one entry's flags with `PayloadStoredDurable` set, leaving its payload in place.
///
/// A payload-less put on a full match keeps the entry's `pack_file`, and the local store derives
/// `PayloadStoredLocal` from it, so only the durable bit changes.
///
/// Returns the put's future with only the error mapped, rather than holding the arguments
/// beside it.
fn mark_entry(
    store: &Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    mut fragment: Fragment,
) -> impl Future<Output = Result<(), StorageError>> + Send + 'static {
    fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
    let put = store.clone().put(partition, address, fragment, None, false);
    async move { put.await.forward("marking the local entry durable") }
}

/// Emit the item's terminal event and return the outcome that was sent.
fn emit_complete(
    item: &LoreStorageMarkDurableItem,
    already_durable: u8,
    marked: u64,
    result: Result<(), StorageError>,
) -> Result<(), StorageError> {
    let address = if result.is_ok() {
        item.address
    } else {
        Address::default()
    };
    LoreEvent::StorageMarkDurableItemComplete(LoreStorageMarkDurableItemCompleteEventData {
        id: item.id,
        address,
        already_durable,
        marked,
        error: item_detail(&result),
    })
    .send();
    result
}
