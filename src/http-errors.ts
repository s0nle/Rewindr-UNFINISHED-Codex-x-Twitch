// Only deliberately constructed messages are safe to expose for server failures.
export class HttpError extends Error {
  constructor(readonly statusCode: number, message: string) { super(message); }
}
