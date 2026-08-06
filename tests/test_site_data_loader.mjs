import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import { gzipSync } from "node:zlib";


test("compressed data loader rewrites only large JSON payloads", async () => {
  const original = {
    document: globalThis.document,
    fetch: globalThis.fetch,
    location: globalThis.location,
    window: globalThis.window,
  };
  const requested = [];
  const payload = Buffer.from(JSON.stringify({ ok: true, value: 42 }));

  try {
    globalThis.window = globalThis;
    globalThis.document = { baseURI: "https://example.test/app/" };
    globalThis.location = new URL("https://example.test/app/");
    globalThis.fetch = async (request) => {
      const url = request instanceof Request ? request.url : String(request);
      requested.push(url);
      if (url.endsWith("server-decoded.json.gz")) {
        return new Response(payload, {
          headers: { "content-encoding": "gzip" },
        });
      }
      if (url.endsWith("raw-only.json.gz")) {
        return new Response("missing compressed payload", { status: 404 });
      }
      if (url === "data/funds/raw-only.json") {
        return new Response(payload, {
          headers: { "content-type": "application/json" },
        });
      }
      if (url.endsWith(".json.gz")) {
        return new Response(gzipSync(payload), {
          headers: { "content-type": "application/gzip" },
        });
      }
      return new Response("plain");
    };

    vm.runInThisContext(
      readFileSync(
        new URL("../site-data-loader.js", import.meta.url),
        "utf8",
      ),
    );

    for (const input of [
      "data/funds/1.json",
      new URL("data/stocks/ABC.json", document.baseURI),
      new Request("https://example.test/app/data/funds/2.json"),
      "data/funds/server-decoded.json",
    ]) {
      const decoded = await (await fetch(input)).json();
      assert.deepEqual(decoded, { ok: true, value: 42 });
    }

    assert.deepEqual(
      await (await fetch("data/funds/raw-only.json")).json(),
      { ok: true, value: 42 },
    );
    assert.equal(await (await fetch("data/index.json")).text(), "plain");
    assert.deepEqual(requested, [
      "https://example.test/app/data/funds/1.json.gz",
      "https://example.test/app/data/stocks/ABC.json.gz",
      "https://example.test/app/data/funds/2.json.gz",
      "https://example.test/app/data/funds/server-decoded.json.gz",
      "https://example.test/app/data/funds/raw-only.json.gz",
      "data/funds/raw-only.json",
      "data/index.json",
    ]);
  } finally {
    globalThis.document = original.document;
    globalThis.fetch = original.fetch;
    globalThis.location = original.location;
    globalThis.window = original.window;
    delete globalThis.__SIS_COMPRESSED_DATA_LOADER__;
  }
});
