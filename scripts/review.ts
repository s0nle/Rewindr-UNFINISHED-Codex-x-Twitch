import { readFile, writeFile } from 'node:fs/promises';
import { assessReview } from '../src/evaluation.js';
async function main() {
  const [reportPath, reviewPath] = process.argv.slice(2);
  if (!reportPath || !reviewPath) throw new Error('Usage: npm run evaluate:review -- report.json review.json');
  const result = assessReview(JSON.parse(await readFile(reportPath, 'utf8')), JSON.parse(await readFile(reviewPath, 'utf8')));
  await writeFile(`${reportPath}.assessment.json`, JSON.stringify(result, null, 2), { flag: 'wx' });
  console.log(JSON.stringify(result, null, 2));
  if (result.gate !== 'passed') process.exitCode = 1;
}
main().catch(error => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
