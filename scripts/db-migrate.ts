import { createHash } from 'node:crypto';
import { readFile, readdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { databaseClient, safeDatabaseError } from '../src/db-connection.js';

async function migrate() {
  const client = await databaseClient();
  try {
    await client.connect();
    await client.query('begin');
    await client.query("select pg_advisory_xact_lock(hashtext('rewindr-schema-migrations'))");
    await client.query('create schema if not exists rewindr_private');
    await client.query('revoke all on schema rewindr_private from public, anon, authenticated');
    await client.query('create table if not exists rewindr_private.schema_migrations(version text primary key, sha256 text not null, applied_at timestamptz not null default now())');
    const directory = resolve('supabase/migrations');
    let applied = 0;
    for (const file of (await readdir(directory)).filter(name => /^\d+_[a-z0-9_]+\.sql$/.test(name)).sort()) {
      const sql = await readFile(resolve(directory, file), 'utf8');
      const hash = createHash('sha256').update(sql).digest('hex');
      const existing = await client.query<{ sha256: string }>('select sha256 from rewindr_private.schema_migrations where version=$1', [file]);
      if (existing.rows.length) {
        if (existing.rows[0]!.sha256 !== hash) throw new Error('An applied migration has changed; create a new migration instead.');
        continue;
      }
      await client.query(sql);
      await client.query('insert into rewindr_private.schema_migrations(version,sha256) values($1,$2)', [file, hash]);
      applied++;
    }
    await client.query("notify pgrst, 'reload schema'");
    await client.query('commit');
    console.log(`Database schema ready: ${applied} migration(s) applied. Existing application data was not reset.`);
  } catch (error) { await client.query('rollback').catch(() => {}); throw error; }
  finally { await client.end(); }
}
migrate().catch(error => { console.error(safeDatabaseError(error)); process.exitCode = 1; });
