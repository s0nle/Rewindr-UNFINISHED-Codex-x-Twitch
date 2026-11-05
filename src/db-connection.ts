import { readFile } from 'node:fs/promises';
import { X509Certificate } from 'node:crypto';
import pg from 'pg';

export async function databaseClient(env: NodeJS.ProcessEnv = process.env) {
  if (!env.SUPABASE_DB_URL || !env.SUPABASE_URL) throw new Error('Set SUPABASE_DB_URL and SUPABASE_URL before running database commands.');
  let url: URL, api: URL;
  try { url = new URL(env.SUPABASE_DB_URL); api = new URL(env.SUPABASE_URL); }
  catch { throw new Error('Invalid database/API URL syntax. Keep credentials in .env.'); }
  if (!['postgres:', 'postgresql:'].includes(url.protocol) || api.protocol !== 'https:') throw new Error('Use a PostgreSQL connection string and HTTPS API URL.');
  const ref = api.hostname.split('.')[0]!;
  const sameProject = url.hostname === `db.${ref}.supabase.co`
    || (url.hostname.endsWith('.pooler.supabase.com') && decodeURIComponent(url.username).endsWith(`.${ref}`));
  if (!sameProject) throw new Error('The database connection and API URL must point to the same Supabase project.');
  // node-postgres URL SSL parameters otherwise replace the explicit CA settings.
  for (const key of ['sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'uselibpqcompat']) url.searchParams.delete(key);
  let ca: string | undefined;
  if (env.SUPABASE_DB_CA_PATH) {
    try { ca = await readFile(env.SUPABASE_DB_CA_PATH, 'utf8'); new X509Certificate(ca); }
    catch { throw new Error('Cannot read the Supabase CA certificate. Check SUPABASE_DB_CA_PATH and save .env.'); }
  }
  return new pg.Client({ connectionString: url.toString(), ssl: { rejectUnauthorized: true, ...(ca ? { ca } : {}) },
    connectionTimeoutMillis: 10000, query_timeout: 20000, statement_timeout: 20000 });
}
export function safeDatabaseError(error: unknown) {
  const code = (error as { code?: unknown })?.code;
  if (typeof code === 'string' && /^[A-Z0-9_]{2,60}$/.test(code)) return `Database operation failed (${code}). Check connection, certificate, permissions, and migration status.`;
  return 'Database operation failed. Check saved environment settings and migration status; credentials are not printed.';
}
