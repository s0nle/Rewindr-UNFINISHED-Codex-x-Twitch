import { z } from 'zod';
const range = z.object({ startSeconds: z.number().finite().nonnegative(), endSeconds: z.number().finite().positive() }).refine(r => r.endSeconds > r.startSeconds, 'Range end must follow start');
const query = z.object({ id: z.string().min(1), question: z.string().trim().min(1), kind: z.enum(['positive', 'absent']), visualOnly: z.boolean(), manuallyVerified: z.literal(true), expectedRanges: z.array(range), distractorRanges: z.array(range), notes: z.string().min(1) });
export const manifestSchema = z.object({
  version: z.literal(1), recordingId: z.string().uuid(), sourceSha256: z.string().regex(/^[a-f0-9]{64}$/), durationSeconds: z.number().min(1200).max(1800),
  verifiedBy: z.string().trim().min(1), verifiedAt: z.iso.datetime(), queries: z.array(query).length(13)
}).superRefine((m, ctx) => {
  const positives = m.queries.filter(q => q.kind === 'positive');
  if (positives.length !== 10 || m.queries.filter(q => q.kind === 'absent').length !== 3) ctx.addIssue({ code: 'custom', message: 'Require exactly 10 positives and 3 absent-event queries' });
  if (new Set(m.queries.map(q => q.id)).size !== 13) ctx.addIssue({ code: 'custom', message: 'Query IDs must be unique' });
  if (positives.filter(q => q.visualOnly).length < 3 || positives.filter(q => q.distractorRanges.length > 0).length < 2) ctx.addIssue({ code: 'custom', message: 'Include at least 3 visual-only positives and 2 positives with similar-looking distractor ranges' });
  for (const q of m.queries) {
    if ((q.kind === 'positive') !== (q.expectedRanges.length > 0)) ctx.addIssue({ code: 'custom', message: `Invalid expected ranges for ${q.id}` });
    if ([...q.expectedRanges, ...q.distractorRanges].some(r => r.endSeconds > m.durationSeconds)) ctx.addIssue({ code: 'custom', message: `Range outside recording for ${q.id}` });
  }
});
export type TimeRange = z.infer<typeof range>;
export function temporalIoU(a: TimeRange, b: TimeRange) {
  const intersection = Math.max(0, Math.min(a.endSeconds, b.endSeconds) - Math.max(a.startSeconds, b.startSeconds));
  return intersection / (Math.max(a.endSeconds, b.endSeconds) - Math.min(a.startSeconds, b.startSeconds));
}
export const resultSchema = z.object({
  recordingId: z.string().uuid(), sourceSha256: z.string(), provider: z.literal('twelvelabs'), model: z.literal('marengo3.0'), evidence: z.literal('live-provider'),
  moments: z.array(z.object({ rank: z.number().int().positive(), recordingId: z.string().uuid(), videoId: z.string(), startSeconds: z.number().finite().nonnegative(), endSeconds: z.number().finite().positive(), playbackUrl: z.string() }))
});
export const reportSchema = z.object({
  evidence: z.literal('live-provider'), manifest: manifestSchema, capturedAt: z.iso.datetime(), apiBase: z.string().url(),
  queries: z.array(z.object({ id: z.string(), response: resultSchema })).length(13)
});
export const reviewSchema = z.object({
  reviewedBy: z.string().trim().min(1), reviewedAt: z.iso.datetime(),
  queries: z.array(z.object({ id: z.string(), acceptedRank: z.number().int().positive().nullable(), inspectedTopThree: z.literal(true), notes: z.string().trim().min(1) })).length(13)
});
export function assessReview(reportInput: unknown, reviewInput: unknown) {
  const report = reportSchema.parse(reportInput);
  const review = reviewSchema.parse(reviewInput);
  if (new Set(review.queries.map(q => q.id)).size !== 13 || new Set(report.queries.map(q => q.id)).size !== 13) throw new Error('Duplicate query IDs');
  let hits = 0;
  const negatives: { id: string; candidatesReturned: number; notes: string }[] = [];
  for (const q of report.manifest.queries) {
    const result = report.queries.find(r => r.id === q.id)?.response;
    const decision = review.queries.find(r => r.id === q.id);
    if (!result || !decision) throw new Error(`Missing result or review for ${q.id}`);
    if (result.recordingId !== report.manifest.recordingId || result.sourceSha256 !== report.manifest.sourceSha256 || result.moments.some(m => m.recordingId !== report.manifest.recordingId)) throw new Error('Source mismatch');
    const topThree = [...result.moments].sort((a, b) => a.rank - b.rank).slice(0, 3);
    if (q.kind === 'absent') {
      if (decision.acceptedRank !== null) throw new Error('An absent event cannot have a correct encounter');
      negatives.push({ id: q.id, candidatesReturned: result.moments.length, notes: decision.notes });
    } else if (decision.acceptedRank !== null) {
      const accepted = topThree.find(m => m.rank === decision.acceptedRank);
      if (!accepted || !q.expectedRanges.some(r => temporalIoU(r, accepted) >= 0.3)) throw new Error(`Reviewed hit ${q.id} must be in the top three and overlap a verified range with IoU >= 0.3`);
      hits++;
    }
  }
  return { evidence: report.evidence, positiveHitsAt3: hits, positiveTotal: 10, gate: hits >= 8 ? 'passed' : 'failed', matchingRule: 'Human-confirmed encounter in top 3 with temporal IoU >= 0.3', negatives, uncertainty: 'Absent queries may return plausible distractors. No calibrated confidence or reliable absence detection is available.' };
}
