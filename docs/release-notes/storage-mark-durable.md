---
type: feature
---

- `lore`: `lore_storage_mark_durable` and `lore_storage_mark_durable_async` mark locally stored
  content durable without uploading it, for a caller that keeps a local and a remote store as two
  handles and copies content between them itself. Each item's local entry, and for fragmented
  content every leaf and nested list below it, is marked durable, so eviction can reclaim its
  payload and upload and push treat it as already durable. A list already durable whose payload
  the store no longer holds is skipped, with nothing below it visited. The mark is kept per
  partition and does not record which remote holds the content, so content should be marked
  only if every remote the store is used with under that partition holds it. Each item emits
  `LORE_EVENT_STORAGE_MARK_DURABLE_ITEM_COMPLETE` with `already_durable`, the number of entries
  `marked`, and its error.
