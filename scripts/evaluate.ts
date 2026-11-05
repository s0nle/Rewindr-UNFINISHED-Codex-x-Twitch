import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import type { z } from 'zod';
import { manifestSchema, resultSchema, temporalIoU } from '../src/evaluation.js';

async function main() {
  const manifestPath = process.argv[2];
  if (!manifestPath) throw new Error('Usage: npm run evaluate -- evaluation/manifest.local.json [http://127.0.0.1:3000]');
  const manifest = manifestSchema.parse(JSON.parse(await readFile(resolve(manifestPath), 'utf8')));
  const base = new URL(process.argv[3] || 'http://127.0.0.1:3000');
  if (base.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(base.hostname) || base.username || base.password) throw new Error('Evaluation accepts a local API only.');
  const statusResponse = await fetch(new URL(`/recordings/${manifest.recordingId}/status`, base), { signal: AbortSignal.timeout(10_000) });
  if (!statusResponse.ok) throw new Error(`Status returned ${statusResponse.status}`);
  const status = await statusResponse.json() as { phase: string; sha256: string; media?: { durationSeconds: number } };
  if (status.phase !== 'ready' || status.sha256 !== manifest.sourceSha256 || Math.abs((status.media?.durationSeconds ?? 0) - manifest.durationSeconds) > 1) throw new Error('Recording must be ready and match the manually verified source hash and duration.');
  const queries: { id: string; response: z.infer<typeof resultSchema> }[] = [];
  for (const q of manifest.queries) {
    const response = await fetch(new URL(`/recordings/${manifest.recordingId}/search`, base), { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ question: q.question, limit: 10 }), signal: AbortSignal.timeout(120_000) });
    if (!response.ok) throw new Error(`Search ${q.id} returned HTTP ${response.status}; no quality gate was evaluated.`);
    const result = resultSchema.parse(await response.json());
    if (result.sourceSha256 !== manifest.sourceSha256 || result.recordingId !== manifest.recordingId) throw new Error('Search source mismatch');
    queries.push({ id: q.id, response: result });
    const topThree = [...result.moments].sort((a,b) => a.rank - b.rank).slice(0,3);
    console.log(`${q.id}: ${result.moments.length} candidates; temporal overlap in top 3: ${q.expectedRanges.some(r => topThree.some(m => temporalIoU(r,m) >= 0.3))}. Human review required.`);
  }
  const capturedAt = new Date().toISOString();
  const report = { evidence: 'live-provider', capturedAt, apiBase: base.origin, manifest, queries };
  const folder = resolve('evaluation/reports');
  await mkdir(folder, { recursive: true });
  const prefix = resolve(folder, capturedAt.replaceAll(/[:.]/g, '-'));
  await writeFile(`${prefix}.json`, JSON.stringify(report, null, 2), { flag: 'wx' });
  const review = { reviewedBy: '', reviewedAt: null, queries: manifest.queries.map(q => ({ id: q.id, acceptedRank: null, inspectedTopThree: false, notes: '' })) };
  await writeFile(`${prefix}.review.json`, JSON.stringify(review, null, 2), { flag: 'wx' });
  const esc = (v: string) => v.replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]!));
  const html = `<!doctype html><html lang="en"><meta charset="utf-8"><title>Rewindr retrieval review</title><style>body{font:16px system-ui;max-width:1000px;margin:40px auto;background:#111;color:#eee}video{width:420px;max-width:100%}article{border-top:1px solid #555;padding:20px 0}a{color:#9cf}</style><h1>Rewindr: human review required</h1><p>These are ranked candidates, not confirmed encounters. Review the top three for each query, then fill in the review JSON.</p>${manifest.queries.map(q => `<article><h2>${esc(q.id)} · ${esc(q.question)}</h2><p>${esc(q.kind)} · Expected: ${esc(JSON.stringify(q.expectedRanges))} · Distractors: ${esc(JSON.stringify(q.distractorRanges))}</p>${queries.find(r=>r.id===q.id)!.response.moments.slice(0,3).map(m=>{ const url = new URL(m.playbackUrl, base); if (url.origin !== base.origin || !url.pathname.startsWith('/recordings/')) throw new Error('Invalid playback URL'); return `<p>Rank ${m.rank}: ${m.startSeconds}–${m.endSeconds}s · <a href="${esc(url.href)}">Open source</a></p><video controls preload="none" src="${esc(url.href)}"></video>`; }).join('')}</article>`).join('')}</html>`;
  await writeFile(`${prefix}.html`, html, { flag: 'wx' });
  console.log(`Saved ${prefix}.json, .html and .review.json. Quality gate: PENDING MANUAL REVIEW.`);
}
main().catch(error => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
