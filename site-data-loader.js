/*
 * GitHub Pages publishes the large per-fund and per-security payloads as
 * deterministic .json.gz files. Keep the application code and public URLs
 * simple by transparently mapping those two data directories to their
 * compressed counterparts.
 *
 * Load this file before the application's inline script in index.html.
 */
(() => {
  "use strict";

  if (window.__SIS_COMPRESSED_DATA_LOADER__) return;

  const nativeFetch = window.fetch.bind(window);
  const compressedDataPath =
    /\/data\/(?:funds|stocks)\/[^/?#]+\.json$/;

  function compressedURL(input) {
    const raw =
      input instanceof Request
        ? input.url
        : input instanceof URL
          ? input.href
          : input;
    if (typeof raw !== "string") return null;

    const url = new URL(raw, document.baseURI);
    if (url.origin !== window.location.origin) return null;
    if (!compressedDataPath.test(url.pathname)) return null;

    url.pathname += ".gz";
    return url;
  }

  function requestFor(input, init, url) {
    if (input instanceof Request) {
      const request = new Request(url, input);
      return init === undefined ? request : new Request(request, init);
    }
    return new Request(url, init);
  }

  async function compressedDataFetch(input, init) {
    const url = compressedURL(input);
    if (url === null) return nativeFetch(input, init);

    const request = requestFor(input, init, url);
    if (request.method !== "GET") return nativeFetch(input, init);

    const response = await nativeFetch(request);
    // During the one-time transition from branch-based Pages, and in local
    // development, only the raw JSON may exist. A compressed 404 is proof that
    // this specific payload is unavailable, so retry its original URL once.
    // Other failures remain visible instead of being masked.
    if (response.status === 404) return nativeFetch(input, init);
    if (!response.ok || response.body === null) return response;

    const headers = new Headers(response.headers);
    const serverDecoded =
      (headers.get("content-encoding") || "")
        .toLowerCase()
        .split(",")
        .map((value) => value.trim())
        .includes("gzip");

    let body = response.body;
    if (!serverDecoded) {
      if (typeof DecompressionStream !== "function") {
        throw new Error(
          "This browser cannot read the compressed 13F dataset. " +
            "Please use a current version of Safari, Chrome, Firefox, or Edge."
        );
      }
      body = body.pipeThrough(new DecompressionStream("gzip"));
    }

    headers.set("content-type", "application/json; charset=utf-8");
    headers.delete("content-encoding");
    headers.delete("content-length");

    return new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  }

  window.fetch = compressedDataFetch;
  window.__SIS_COMPRESSED_DATA_LOADER__ = Object.freeze({
    version: 1,
    nativeFetch,
  });
})();
