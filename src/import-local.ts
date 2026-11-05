import type { Persistence } from './persistence.js';
import type { Broadcast } from './types.js';
import { ImportValidationError } from './import-errors.js';

function canonicalBroadcast(value: Broadcast) {
  return JSON.stringify({ ...value, createdAt: new Date(value.createdAt).toISOString(),
    chunks: [...value.chunks].sort((a, b) => a.startSeconds - b.startSeconds) });
}
export async function importLocalMetadata(local: Persistence, remote: Persistence, apply: boolean) {
  const recordings = await local.recordings.all();
  const broadcasts = await local.broadcasts.all();
  const pendingRecordings = [];
  const pendingBroadcasts = [];
  const staleRecordings: { recordingId: string; localPhase: string; remotePhase: string; localUpdatedAt: string; remoteUpdatedAt: string }[] = [];
  for (const record of recordings) {
    const existing = await remote.recordings.get(record.id);
    if (!existing) pendingRecordings.push(record);
    else if (existing.sha256 !== record.sha256 || existing.bytes !== record.bytes || existing.filename !== record.filename) throw new ImportValidationError(`Recording identity conflict for ${record.id}; no import writes were started.`);
    else if (Date.parse(existing.updatedAt) < Date.parse(record.updatedAt)) staleRecordings.push({ recordingId: record.id,
      localPhase: record.phase, remotePhase: existing.phase, localUpdatedAt: record.updatedAt, remoteUpdatedAt: existing.updatedAt });
  }
  for (const broadcast of broadcasts) {
    const existing = await remote.broadcasts.get(broadcast.id);
    if (!existing) pendingBroadcasts.push(broadcast);
    else if (canonicalBroadcast(existing) !== canonicalBroadcast(broadcast)) throw new ImportValidationError(`Broadcast mapping conflict for ${broadcast.id}; no import writes were started.`);
    if (broadcast.sourceRecordingId && !recordings.some(r => r.id === broadcast.sourceRecordingId) && !await remote.recordings.get(broadcast.sourceRecordingId)) throw new ImportValidationError(`Broadcast source is missing for ${broadcast.id}: recording ${broadcast.sourceRecordingId}; no import writes were started.`);
  }
  if (apply) {
    for (const record of pendingRecordings) await remote.recordings.put(record);
    for (const broadcast of pendingBroadcasts) await remote.broadcasts.put(broadcast);
  }
  return { mode: apply ? 'applied' : 'dry-run', recordings: pendingRecordings.length, broadcasts: pendingBroadcasts.length,
    alreadyPresent: recordings.length + broadcasts.length - pendingRecordings.length - pendingBroadcasts.length - staleRecordings.length,
    skippedStale: staleRecordings.length, staleRecordings };
}
