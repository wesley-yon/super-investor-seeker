// Deploy only as a route in front of 13f.wesleyyon.com. The existing DNS,
// origin and Bot Fight Mode remain in place. See CSP-OWNER-SETUP.md.
export function contentSecurityPolicy(nonce) {
  if (!/^[A-Za-z0-9+/]{22}==$/.test(nonce)) throw new Error('Invalid CSP nonce');
  return [
    "default-src 'self'",
    "base-uri 'none'",
    "object-src 'none'",
    `script-src 'self' 'nonce-${nonce}' https://static.cloudflareinsights.com`,
    "script-src-attr 'none'",
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    "font-src 'self' https://fonts.gstatic.com",
    "img-src 'self' data:",
    "connect-src 'self' https://cloudflareinsights.com",
    "frame-ancestors 'self'",
    "form-action 'none'",
  ].join('; ');
}

export default {
  async fetch(request, env = {}) {
    const response = await fetch(request);
    const url = new URL(request.url);
    if (url.hostname !== '13f.wesleyyon.com' || url.protocol !== 'https:' ||
        !response.ok || request.method !== 'GET' ||
        !/^text\/html(?:\s*;|$)/i.test(response.headers.get('content-type') || '')) {
      return response;
    }
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    const nonce = btoa(String.fromCharCode(...bytes));
    const result = new Response(response.body, response);
    const header = env.CSP_MODE === 'enforce'
      ? 'Content-Security-Policy' : 'Content-Security-Policy-Report-Only';
    result.headers.set(header, contentSecurityPolicy(nonce));
    // Prevent shared/downstream HTML caching from reusing a response nonce.
    // Static assets and compressed datasets retain their original caching.
    result.headers.set('Cache-Control', 'no-store');
    result.headers.delete('ETag');
    result.headers.delete('Last-Modified');
    return result;
  },
};
