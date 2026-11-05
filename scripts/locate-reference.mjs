// Local pixel alignment of a supplied excerpt with its source. This does not
// detect people or train a model; it only recovers the excerpt's source offset.
import { spawnSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import ffmpeg from 'ffmpeg-static';
const [source,reference,output]=process.argv.slice(2);
if(!source||!reference)throw new Error('Usage: node scripts/locate-reference.mjs source.mp4 reference.mp4 [output.json]');
function frames(path){const r=spawnSync(ffmpeg,['-nostdin','-v','error','-i',path,'-vf','fps=1,scale=32:18,format=gray','-f','rawvideo','pipe:1'],{maxBuffer:32*1024*1024,windowsHide:true});if(r.status!==0)throw new Error('Cannot decode alignment samples');return r.stdout;}
const src=frames(source),ref=frames(reference),size=32*18;
const offsets=[];
const samples=[2,4,6,8,10,12].filter(t=>(t+1)*size<=ref.length);
for(let offset=0;offset<Math.floor(src.length/size)-Math.ceil(ref.length/size);offset++){
  let difference=0;
  for(const t of samples)for(let px=0;px<size;px++)difference+=Math.abs(src[(offset+t)*size+px]-ref[t*size+px]);
  offsets.push({startSeconds:offset,meanPixelDifference:difference/(samples.length*size)});
}
offsets.sort((a,b)=>a.meanPixelDifference-b.meanPixelDifference);
const result={method:'local pixel alignment; inspect source to confirm',referenceDurationApprox:Math.floor(ref.length/size),bestOffsets:offsets.slice(0,5)};
if(output)writeFileSync(output,JSON.stringify(result,null,2));
console.log(JSON.stringify(result));
