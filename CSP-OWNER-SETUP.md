# Finish Content Security Policy on Cloudflare

Status: application changes and Worker are prepared and tested locally. No
Cloudflare access or deployment has been performed.

The application now uses external JavaScript and native links/buttons. Its
HTML policy blocks inline event handlers, objects, base-URL changes and form
submissions. Full script-element restrictions need an HTTP response header:
Bot Fight Mode injects its own inline script and Cloudflare supports its nonce
only through a CSP header, not an HTML meta tag.
[Cloudflare compatibility requirements](https://developers.cloudflare.com/bots/get-started/bot-fight-mode/#javascript-detections)

## Before applying

Merge and verify the source PR first. The old site uses inline application code,
so enforcing this policy before that update would break it.

This adds a Cloudflare Worker on the existing host, without changing DNS or
turning off Bot Fight Mode. Worker usage counts against your account plan.
Workers Free permits 100,000 requests per day; the route below includes asset
and data requests. Check available capacity before adding it. A fail-closed
route can return an error when that quota is exhausted. No plan upgrade or
billing change has been authorized.
[Worker limits](https://developers.cloudflare.com/workers/platform/limits/#daily-requests)

## Apply in report-only mode

1. In **Cloudflare → Workers & Pages**, create a Worker named **13f-csp**.
   Open its code editor and replace the sample code with
   [cloudflare/csp-worker.mjs](cloudflare/csp-worker.mjs). Deploy the Worker.
2. Under its **Settings → Variables and Secrets**, add a plain-text variable
   **CSP_MODE** with value **report-only**. The code also defaults to this mode.
3. Under **Settings → Domains & Routes**, add a **Route** (not a Custom Domain):
   **`https://13f.wesleyyon.com/*`**, zone **wesleyyon.com**. If that route already
   belongs to another Worker, stop and combine the logic in that Worker first.
   Preserve the existing DNS target, proxy, SSL mode and security-header rule.
   Do not choose a route covering your apex or other subdomains.
4. Open the homepage, a fund page and a security page in a normal browser. Check
   the Network panel's successful HTML response for
   **Content-Security-Policy-Report-Only**, including a different nonce on fresh
   requests. Check Console for policy violations, especially Cloudflare's
   injected detection script. Report-only mode records console violations; it
   does not block scripts and does not send reports to a third party.

[Cloudflare Worker routes](https://developers.cloudflare.com/workers/configuration/routing/routes/)

## Enforce after verification

After the report-only checks pass, change **CSP_MODE** to **enforce** and deploy
that configuration. Repeat homepage/search/fund/security navigation. Confirm a
successful HTML response contains **Content-Security-Policy** and that the
Cloudflare injected script has a matching nonce and executes without violations.
Only then mark the full script policy deployed. Local tests simulate nonce
handling; they cannot prove Cloudflare's live injection/configuration.

If enforcement causes a problem, return **CSP_MODE** to **report-only** while
reviewing the violation. This leaves existing TLS, Bot Fight Mode and security
headers in place. Do not add `unsafe-inline` to script-src or use a fixed nonce.

The Worker permits the existing Google fonts and Cloudflare analytics endpoints.
Inline CSS remains allowed because the interface uses dynamic inline styles;
inline JavaScript handlers and eval remain prohibited. It changes only successful
HTTPS GET HTML responses on the exact site host and streams the original body.
HTML responses become `no-store` to prevent nonce reuse; dataset/asset cache
headers remain unchanged. The Worker uses no secrets or external storage.
[Cloudflare analytics CSP requirements](https://developers.cloudflare.com/web-analytics/faq/#what-do-i-need-to-add-to-my-content-security-policy)
