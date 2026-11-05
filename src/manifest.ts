import { chunkFilenameSchema } from './filenames.js';

// Both manifest formats use CSV quoting. Do not split a quoted filename's commas.
// Line breaks in filenames are intentionally unsupported and rejected.
function parseRow(line: string): string[] {
  const fields: string[] = [];
  let value = '';
  let quoted = false;
  let closedQuote = false;
  for (let i = 0; i < line.length; i++) {
    const character = line[i]!;
    if (quoted) {
      if (character !== '"') value += character;
      else if (line[i + 1] === '"') { value += '"'; i++; }
      else { quoted = false; closedQuote = true; }
    } else if (character === ',') {
      fields.push(value); value = ''; closedQuote = false;
    } else if (character === '"') {
      if (value.length || closedQuote) throw new Error('Invalid CSV quoting in manifest.');
      quoted = true;
    } else if (closedQuote) {
      if (character !== ' ' && character !== '\t') throw new Error('Unexpected text after a quoted CSV value.');
    } else value += character;
  }
  if (quoted) throw new Error('Unclosed CSV quote in manifest.');
  fields.push(value);
  return fields;
}

export function parseChunkManifest(text: string) {
  const lines = text.replace(/^\uFEFF/, '').split(/\r?\n/).filter(line => line.trim());
  const rows = lines.map(parseRow);
  if (rows[0]?.[0] === 'filename') {
    if (rows[0].join(',') !== 'filename,start_seconds,end_seconds') throw new Error('Unexpected manifest header.');
    rows.shift();
  }
  if (!rows.length) throw new Error('Manifest is empty.');
  return rows.map((row, index) => {
    if (row.length !== 3 || !row[0] || !row[1]?.trim() || !row[2]?.trim()) throw new Error(`Invalid manifest row ${index + 1}.`);
    const startSeconds = Number(row[1]);
    const endSeconds = Number(row[2]);
    if (!Number.isFinite(startSeconds) || !Number.isFinite(endSeconds) || startSeconds < 0 || endSeconds <= startSeconds) {
      throw new Error(`Invalid times in manifest row ${index + 1}.`);
    }
    const filename = chunkFilenameSchema.safeParse(row[0]);
    if (!filename.success) throw new Error(`Invalid MP4 filename in manifest row ${index + 1}.`);
    return { filename: filename.data, startSeconds, endSeconds };
  });
}
