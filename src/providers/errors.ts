export interface ProviderErrorDetails { operation: string; httpStatus?: number; code?: string }
export function isTransientProviderError(error:unknown):boolean {
  const e=error as {statusCode?:number;status?:number;retryable?:boolean;name?:string;code?:string;cause?:{code?:string}}|null;
  const status=e?.statusCode??e?.status;return Boolean(e?.retryable||status===408||status===429||(status!==undefined&&status>=500)||
    ['TimeoutError','AbortError','TwelvelabsApiTimeoutError'].includes(e?.name??'')||/^(ECONNRESET|ECONNREFUSED|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|UND_ERR_)/.test(e?.cause?.code??e?.code??''));
}
export function describeProviderFailure(error: unknown, operation: string): { message: string; details: ProviderErrorDetails } {
  const err = error as { statusCode?: unknown; body?: { code?: unknown }; name?: string; cause?: { code?: string } } | null;
  const httpStatus = typeof err?.statusCode === 'number' ? err.statusCode : undefined;
  const code = typeof err?.body?.code === 'string' && /^[a-zA-Z0-9_-]{1,100}$/.test(err.body.code) ? err.body.code : undefined;
  let message: string;
  if (code === 'asset_storage_limit_exceeded') message = 'TwelveLabs storage capacity exceeded for your plan. Use a smaller video file or increase your TwelveLabs storage capacity.';
  else if (httpStatus === 401 || httpStatus === 403) message = 'TwelveLabs rejected authentication or access. Check the saved API key and account permissions.';
  else if (httpStatus === 429) message = 'TwelveLabs rate or usage limit reached. Check your account allowance before retrying.';
  else if (err?.name === 'ZodError') message = 'TwelveLabs returned an unexpected response format. The provider adapter needs an update.';
  else if (err?.cause?.code) message = 'Cannot reach TwelveLabs. Check your internet connection and try again.';
  else message = 'TwelveLabs operation failed. Existing identifiers are retained; inspect the provider resources before resubmitting.';
  const suffix = [httpStatus ? `HTTP ${httpStatus}` : '', code].filter(Boolean).join(', ');
  return { message: `${message} Failed during ${operation}${suffix ? ` (${suffix})` : ''}.`, details: { operation, httpStatus, code } };
}
