import { HttpError } from './http-errors.js';

// Deliberately authored messages may contain record IDs, never raw provider errors
// or parsed input that could contain passwords/upload credentials.
export class ImportValidationError extends Error {}
export function describeImportError(error: unknown) {
  if (error instanceof ImportValidationError || error instanceof HttpError) return `Metadata import stopped: ${error.message}`;
  return 'Metadata import stopped. Check schema, connection, owner identity and local metadata. Local files were retained; credentials are not printed.';
}
