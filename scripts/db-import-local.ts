import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createLocalPersistence } from '../src/local-persistence.js';
import { createSupabasePersistence } from '../src/supabase-persistence.js';
import { importLocalMetadata } from '../src/import-local.js';
import { ImportValidationError, describeImportError } from '../src/import-errors.js';

async function run() {
  if (process.argv.slice(2).some(arg => arg !== '--apply')) throw new ImportValidationError('Use --apply to import; omit it for a dry run.');
  const root = resolve(process.env.DATA_DIR || '.local/rewindr');
  try {
    const pid = Number(await readFile(resolve(root, 'api.lock'), 'utf8'));
    if (!Number.isSafeInteger(pid) || pid <= 0) throw new ImportValidationError('Invalid API lock.');
    try { process.kill(pid, 0); throw new ImportValidationError('Stop the API before importing metadata.'); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error; }
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  const local = createLocalPersistence(root);
  const remote = createSupabasePersistence(root);
  await local.init(); await remote.init();
  console.log(JSON.stringify(await importLocalMetadata(local, remote, process.argv.includes('--apply')), null, 2));
}
run().catch(error => { console.error(describeImportError(error)); process.exitCode = 1; });
