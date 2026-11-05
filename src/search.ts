import type { ProviderMoment } from './types.js';

export function buildEncounters(moments: ProviderMoment[], durationSeconds: number) {
  const groups: { start: number; end: number; matches: ProviderMoment[] }[] = [];
  for (const moment of [...moments].sort((a,b)=>a.startSeconds-b.startSeconds)) {
    const previous=groups.at(-1);
    if(previous && moment.videoId===previous.matches[0]!.videoId && moment.startSeconds<=previous.end+3 && Math.max(previous.end,moment.endSeconds)-previous.start<=90){
      previous.end=Math.max(previous.end,moment.endSeconds);previous.matches.push(moment);
    }else groups.push({start:moment.startSeconds,end:moment.endSeconds,matches:[moment]});
  }
  return groups.sort((a,b)=>Math.min(...a.matches.map(m=>m.rank))-Math.min(...b.matches.map(m=>m.rank))).map((group,index)=>({
    candidateNumber:index+1, bestProviderRank:Math.min(...group.matches.map(m=>m.rank)),
    matchStartSeconds:group.start,matchEndSeconds:group.end,
    startSeconds:Math.max(0,group.start-30),endSeconds:Math.min(durationSeconds,group.end+30),
    providerMatches:group.matches.sort((a,b)=>a.rank-b.rank)
  }));
}
