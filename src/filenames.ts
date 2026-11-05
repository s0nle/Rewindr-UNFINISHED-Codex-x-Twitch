import { z } from 'zod';

// Match display names exactly, including Unicode and punctuation, without accepting
// filesystem paths, control characters, Windows stream syntax or device names.
const forbidden = /[<>:"/\\|?*\p{Cc}\p{Zl}\p{Zp}]/u;
const deviceName = /^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])$/i;
export const chunkFilenameSchema = z.string().min(5).max(200).refine(name => {
  const stem = name.slice(0, -4);
  const firstComponent = name.split('.')[0]!.replace(/[ .]+$/u, '');
  return name.toLowerCase().endsWith('.mp4') && stem.trim().length > 0
    && !forbidden.test(name) && !deviceName.test(firstComponent);
}, 'Use a plain MP4 filename without paths, control characters, or reserved device names.');
