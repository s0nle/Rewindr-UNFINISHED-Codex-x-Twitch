import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import multipart from '@fastify/multipart';
import { TwelvelabsApiClient } from 'twelvelabs-js';
import { TwelveLabsProvider } from '../src/providers/twelvelabs.js';

test('installed SDK serializes multi-chunk scope and modalities, and rejects results outside that scope', async () => {
  const api = Fastify(); await api.register(multipart);
  let wrongSource = false;
  const pages: number[] = [];
  api.get<{ Querystring: { page?: string } }>('/v1.3/indexes/index/indexed-assets', async req => {
    const page = Number(req.query.page ?? 1); pages.push(page);
    return { data: page === 1
      ? [{ _id: 'a', asset_id: 'asset-a', status: 'ready', system_metadata: { filename: 'chunk_000.mp4', duration: 60 } }]
      : page === 2 ? [{ _id: 'b', asset_id: 'asset-b', status: 'ready', system_metadata: { filename: 'chunk_001.mp4', duration: 60 } }] : [] };
  });
  api.post('/v1.3/search', async req => {
    const fields: Record<string, string[]> = {};
    for await (const part of req.parts()) {
      assert.equal(part.type, 'field');
      if (part.type === 'field') (fields[part.fieldname] ??= []).push(String(part.value));
    }
    assert.deepEqual(fields.search_options, ['visual', 'transcription']);
    assert.deepEqual(fields.transcription_options, ['lexical', 'semantic']);
    assert.deepEqual(fields.operator, ['or']);
    assert.deepEqual(fields.filter, [JSON.stringify({ id: ['a', 'b'] })]);
    assert.deepEqual(fields.page_limit, ['50']);
    assert.deepEqual(fields.query_text, ['Original streamer question']);
    return { data: [{ video_id: wrongSource ? 'other' : 'b', start: 2, end: 6, rank: 1 }] };
  });
  const address = await api.listen({ host: '127.0.0.1', port: 0 });
  try {
    const provider = new TwelveLabsProvider(new TwelvelabsApiClient({ apiKey: 'mock-key', baseUrl: `${address}/v1.3` }));
    const catalog = await provider.listIndexedChunks('index');
    assert.equal(catalog.length, 2);
    assert.deepEqual(pages, [1, 2, 3]);
    assert.equal(catalog[1]!.filename, 'chunk_001.mp4');
    assert.equal(catalog[1]!.durationSeconds, 60);
    const settings = { modalities: ['visual', 'transcription'] as ('visual' | 'transcription')[],
      operator: 'or' as const, transcriptionOptions: ['lexical', 'semantic'] as ('lexical' | 'semantic')[] };
    const result = await provider.searchMany('index', ['a', 'b'], 'Original streamer question', 50, settings);
    assert.equal(result[0]!.videoId, 'b');
    wrongSource = true;
    await assert.rejects(provider.searchMany('index', ['a', 'b'], 'Original streamer question', 50, settings), /another source/);
    await assert.rejects(provider.searchMany('index', [], 'Original streamer question', 50, settings), /Invalid scoped/);
  } finally { await api.close(); }
});
