import { mkdir, readFile, readdir, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { LocalStore } from './store.js';
import { broadcastSchema } from './metadata.js';
import type { Broadcast, ChunkStorageRef } from './types.js';
import { HttpError } from './http-errors.js';
import type { BroadcastRepository, Persistence } from './persistence.js';

export class LocalBroadcastStore implements BroadcastRepository {
  private records = new Map<string, Broadcast>();
  private directory: string;
  constructor(root: string) { this.directory = join(root, 'broadcasts'); }
  async init() {
    await mkdir(this.directory, { recursive: true });
    for (const file of await readdir(this.directory)) {
      if (!file.endsWith('.json')) continue;
      const record = broadcastSchema.parse(JSON.parse(await readFile(join(this.directory, file), 'utf8')));
      if (file !== `${record.id}.json`) throw new Error('Invalid broadcast metadata filename.');
      this.records.set(record.id, record);
    }
  }
  async get(id: string) { const value = this.records.get(id); return value && structuredClone(value); }
  async all() { return [...this.records.values()].map(value => structuredClone(value)); }
  async put(value: Broadcast) {
    const record = broadcastSchema.parse(value);
    const path = join(this.directory, `${record.id}.json`);
    await writeFile(`${path}.tmp`, JSON.stringify(record, null, 2), { flag: 'w', mode: 0o600 });
    await rename(`${path}.tmp`, path);
    this.records.set(record.id, record);
  }
  async attachStorage(broadcastId: string, indexedAssetId: string, storage: ChunkStorageRef) {
    const record = await this.get(broadcastId);
    const chunk = record?.chunks.find(value => value.indexedAssetId === indexedAssetId);
    if (!record || !chunk) throw new HttpError(404, 'Chunk not found.');
    if (chunk.storage && chunk.storage.sha256 !== storage.sha256) throw new HttpError(409, 'Different chunk media is already attached.');
    chunk.storage = storage;
    await this.put(record);
  }
}
export function createLocalPersistence(root: string): Persistence {
  const recordings = new LocalStore(root);
  const broadcasts = new LocalBroadcastStore(root);
  return { kind: 'local', recordings, broadcasts,
    async init() { await recordings.init(); await broadcasts.init(); }, async health() {} };
}
