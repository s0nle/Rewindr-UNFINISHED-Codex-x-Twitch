import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import ffprobe from 'ffprobe-static';
import { z } from 'zod';
import type { MediaInfo } from './types.js';
const exec = promisify(execFile);
const probeSchema = z.object({
  format: z.object({ duration: z.coerce.number().finite(), format_name: z.string() }),
  streams: z.array(z.object({ codec_type: z.string(), codec_name: z.string().optional(), width: z.number().optional(), height: z.number().optional(), pix_fmt: z.string().optional() }))
});
async function probe(path: string, binary: string, expectedChunkDuration?: number, ingestionSource=false): Promise<MediaInfo> {
  const { stdout } = await exec(binary, ['-v', 'error', '-protocol_whitelist', 'file', '-show_format', '-show_streams', '-of', 'json', path], { timeout: 30_000, maxBuffer: 1024 * 1024, windowsHide: true });
  const data = probeSchema.parse(JSON.parse(stdout));
  const video = data.streams.find(s => s.codec_type === 'video');
  if (!video?.width || !video.height) throw new Error('The recording must contain a video stream.');
  if(ingestionSource) {
    if(data.format.duration<4||data.format.duration>12*3600)throw Error('Recorded ingestion accepts 4 seconds to 12 hours.');
  } else if (expectedChunkDuration === undefined) {
    if (data.format.duration < 1200 || data.format.duration > 1800) throw new Error('Stage 1 requires one 20–30 minute recording (1200–1800 seconds).');
  } else if (data.format.duration <= 0 || data.format.duration > 66 || Math.abs(data.format.duration - expectedChunkDuration) > 1) {
    throw new Error('Chunk duration does not match the registered manifest.');
  }
  const { width, height } = video;
  if (width < 360 || height < 360 || width > 5184 || height > 2160 || width / height > 2.4 || height / width > 2.4) throw new Error('Video resolution or aspect ratio is outside Marengo 3.0 limits.');
  // Narrow prototype input to a browser-playable source so timestamps can be reviewed directly.
  if (!ingestionSource && (!data.format.format_name.split(',').includes('mp4') || video.codec_name !== 'h264' || video.pix_fmt !== 'yuv420p' || data.streams.some(s => s.codec_type === 'audio' && s.codec_name !== 'aac'))) {
    throw new Error('Stage 1 playback requires MP4 with H.264 yuv420p video and optional AAC audio. See README for conversion.');
  }
  return { durationSeconds: data.format.duration, width, height, videoCodec: video.codec_name||'unknown' };
}
export function probeMedia(path: string, binary = process.env.FFPROBE_PATH || ffprobe.path) { return probe(path, binary); }
export function probeChunkMedia(path: string, expectedDuration: number, binary = process.env.FFPROBE_PATH || ffprobe.path) {
  return probe(path, binary, expectedDuration);
}
export function inspectIngestionSource(path:string,binary=process.env.FFPROBE_PATH||ffprobe.path){return probe(path,binary,undefined,true);}
