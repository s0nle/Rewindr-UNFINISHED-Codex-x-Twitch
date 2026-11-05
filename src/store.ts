import { mkdir, readFile, readdir, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Recording } from './types.js';
import type { RecordingRepository } from './persistence.js';
import { recordingSchema } from './metadata.js';

// Temporary Stage 1 persistence. One API process only; replace with Supabase in Stage 2.
export class LocalStore implements RecordingRepository {
  private records = new Map<string, Recording>();
  constructor(readonly root: string) {}
  mediaPath(id: string) { return join(this.root, 'media', `${id}.mp4`); }
  async init() {
    await mkdir(join(this.root, 'media'), { recursive: true });
    await mkdir(join(this.root, 'records'), { recursive: true });
    for (const file of await readdir(join(this.root, 'records'))) {
      if (!file.endsWith('.json')) continue;
      const record = recordingSchema.parse(JSON.parse(await readFile(join(this.root, 'records', file), 'utf8')));
      if (!/^[0-9a-f-]{36}$/.test(record.id) || file !== `${record.id}.json`) throw new Error('Invalid local recording metadata');
      this.records.set(record.id, record);
    }
  }
  async get(id: string) { const r = this.records.get(id); return r && structuredClone(r); }
  async all() { return [...this.records.values()].map(r => structuredClone(r)); }
  async put(record: Recording) {
    const next = recordingSchema.parse({ ...record, updatedAt: new Date().toISOString() });
    const path = join(this.root, 'records', `${record.id}.json`);
    await writeFile(`${path}.tmp`, JSON.stringify(next, null, 2), { mode: 0o600 });
    await rename(`${path}.tmp`, path);
    this.records.set(record.id, structuredClone(next));
  }
}
