import {test} from 'node:test';
import assert from 'node:assert/strict';
import {queryVariants} from '../src/query-variants.js';
import {selectCandidates,mergePool,planAnalysis,analysisCacheKey,combineAssessments} from '../src/search-ranking.js';
import type {Broadcast} from '../src/types.js';
import type {Assessment,SearchHit} from '../src/search-types.js';
const source:Broadcast={id:'11111111-1111-4111-8111-111111111111',title:'Test',indexId:'index',createdAt:new Date().toISOString(),sourceOffsetSeconds:7200,
  chunks:Array.from({length:5},(_,i)=>({filename:`chunk_${i}.mp4`,indexedAssetId:`video-${i}`,assetId:`asset-${i}`,startSeconds:i*60,endSeconds:(i+1)*60}))};
test('query engineering preserves brands, quoted lyrics, negations and supplied attributes without inventing companions',()=>{
  const original='Guy with a Polo Ralph Lauren hoodie, not a white shirt, asks for a photo';const variants=queryVariants(original);
  assert.ok(variants.length>=4);for(const query of variants){assert.ok(query.includes('Polo Ralph Lauren'));assert.ok(query.includes('not a white shirt'));assert.doesNotMatch(query,/beside|companion|beach|camera/);}
  for(const query of queryVariants('Lady Gaga sings "there you go floating down a wishing well"')){assert.ok(query.includes('Lady Gaga'));assert.ok(query.includes('"there you go floating down a wishing well"'));}
  const floral=queryVariants('Lady with black floral dress and black sunglasses walks by');assert.ok(floral.some(q=>q.includes('flower-patterned')));assert.ok(floral.every(q=>q.includes('black')&&q.includes('sunglasses')));
  assert.deepEqual(queryVariants(original),queryVariants(original));
});
test('rejections hide intervals and shifted duplicates while preserving other encounters in the same minute',()=>{
  const pool:SearchHit[]=[{videoId:'video-0',startSeconds:10,endSeconds:15,rank:1,query:'original'},{videoId:'video-0',startSeconds:11,endSeconds:16,rank:1,query:'variant'},
    {videoId:'video-0',startSeconds:40,endSeconds:45,rank:2,query:'variant'},{videoId:'video-1',startSeconds:10,endSeconds:15,rank:3,query:'original'}];
  const candidates=selectCandidates(source,pool,[{startSeconds:10,endSeconds:15}]);assert.equal(candidates.length,2);assert.equal(candidates[0]!.matchStartSeconds,40);
  assert.ok(candidates.every(c=>c.matchStartSeconds>17));assert.equal(selectCandidates(source,pool,[])[0]!.matchStartSeconds,10);
});
test('fusion deduplicates identical hits within a query and preserves provider order on the original search',()=>{
  const hits=[{videoId:'video-2',startSeconds:10,endSeconds:15,rank:1,query:'q'},{videoId:'video-0',startSeconds:10,endSeconds:15,rank:2,query:'q'},
    {videoId:'video-3',startSeconds:59,endSeconds:60.5,rank:3,query:'q'}];
  const pool=mergePool(hits,[...hits]);assert.equal(pool.length,3);assert.deepEqual(selectCandidates(source,pool,[]).map(c=>c.bestProviderRank),[1,2,3]);
});
test('analysis scopes a boundary encounter to valid parts and caches by footage/query/model rather than random task IDs',()=>{
  const selection=selectCandidates(source,[{videoId:'video-0',startSeconds:59,endSeconds:60,rank:1,query:'q'},{videoId:'video-1',startSeconds:0,endSeconds:1,rank:2,query:'q'}],[])[0]!;
  const parts=planAnalysis(source,selection);assert.equal(parts.length,2);assert.ok(parts.every(p=>p.endSeconds-p.startSeconds>=4));assert.equal(parts[0]!.endSeconds,60);
  const another=planAnalysis(source,selection);assert.notEqual(parts[0]!.id,another[0]!.id);assert.equal(analysisCacheKey(source,'q',selection,parts,'pegasus1.5'),analysisCacheKey(source,'q',selection,another,'pegasus1.5'));
  assert.notEqual(analysisCacheKey(source,'q',selection,parts,'pegasus1.5'),analysisCacheKey(source,'different',selection,parts,'pegasus1.5'));
});
test('mixed analysis evidence remains uncertain and timestamps map back to recording time',()=>{
  const selection=selectCandidates(source,[{videoId:'video-0',startSeconds:59,endSeconds:60,rank:1,query:'q'},{videoId:'video-1',startSeconds:0,endSeconds:1,rank:2,query:'q'}],[])[0]!;
  const plan=planAnalysis(source,selection);const yes:Assessment={verdict:'match',summary:'A person wears sunglasses.',matchingDetails:['Sunglasses'],unclearDetails:[],evidence:[{timestampSeconds:59,detail:'Sunglasses visible',modality:'visual'}]};
  const no:Assessment={...yes,verdict:'no_match',evidence:[{timestampSeconds:1,detail:'Requested clothing is not visible',modality:'visual'}]};
  const combined=combineAssessments([{plan:plan[0]!,assessment:yes},{plan:plan[1]!,assessment:no}]);assert.equal(combined.verdict,'unclear');assert.equal(combined.evidence[1]!.timestampSeconds,61);
});
