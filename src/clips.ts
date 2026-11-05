import { createHash, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { mkdir, rename, rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import ffmpegPackage from 'ffmpeg-static';
const exec = promisify(execFile);
export class ClipExtractor {
  private busy = false;
  constructor(private root: string) {}
  path(recordingId: string, clipId: string) { return join(this.root, 'clips', `${recordingId}-${clipId}.mp4`); }
  async extract(recordingId: string, source: string, start: number, end: number) {
    const clipId = createHash('sha256').update(`${recordingId}:${start}:${end}:h264-v1`).digest('hex');
    const output = this.path(recordingId, clipId);
    try { if ((await stat(output)).size > 0) return clipId; } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    if (this.busy) throw new Error('BUSY');
    this.busy = true;
    const partial = `${output}.${randomUUID()}.partial.mp4`;
    try {
      await mkdir(join(this.root,'clips'),{recursive:true});
      const binary = process.env.FFMPEG_PATH || ffmpegPackage as unknown as string;
      if (!binary) throw new Error('FFmpeg is unavailable');
      // Re-encode after accurate input seeking so cuts are not restricted to keyframes.
      await exec(binary, ['-nostdin','-v','error','-protocol_whitelist','file','-ss',String(start),'-i',source,'-t',String(end-start),'-map','0:v:0','-map','0:a:0?','-c:v','libx264','-preset','veryfast','-crf','20','-pix_fmt','yuv420p','-c:a','aac','-movflags','+faststart',partial], { timeout: 180_000, maxBuffer: 1024 * 1024, windowsHide: true });
      await rename(partial,output);
      return clipId;
    } finally { this.busy=false; await rm(partial,{force:true}); }
  }
}
