/**
 * Short description of an error's cause, for logs. fetch() reports network failures as
 * "fetch failed" and keeps the reason (ECONNRESET, a timeout, ...) in `cause`.
 * Returns only codes and names, never messages, so no request detail is logged.
 */
export function errorCause(err: unknown): string | undefined {
  const cause = (err as { cause?: unknown } | null)?.cause as
    | { code?: unknown; name?: unknown; errors?: unknown }
    | undefined;
  if (!cause || typeof cause !== 'object') return undefined;
  if (typeof cause.code === 'string') return cause.code;
  if (Array.isArray(cause.errors)) {
    const codes = cause.errors
      .map((e) => (e as { code?: unknown })?.code)
      .filter((c): c is string => typeof c === 'string');
    if (codes.length > 0) return [...new Set(codes)].join(',');
  }
  return typeof cause.name === 'string' ? cause.name : undefined;
}
