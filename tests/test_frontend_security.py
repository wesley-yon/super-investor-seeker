"""Browser trust boundaries and accessible controls independent of data values."""
import json
import re
import subprocess
import unittest
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class Elements(HTMLParser):
    def __init__(self, markup):
        super().__init__()
        self.nodes = []
        self.feed(markup)

    def handle_starttag(self, tag, attrs):
        self.nodes.append((tag, dict(attrs)))


class FrontendSecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (ROOT / "index.html").read_text()
        cls.code = (ROOT / "app.js").read_text()
        a = cls.code.index("// ---------- init ----------")
        b = cls.code.index("// ---------- URL routing ----------")
        cls.without_init = cls.code[:a] + cls.code[b:]

    def js(self, body):
        result = subprocess.run(["node", "-e", self.without_init + "\n" + body],
                                capture_output=True, text=True, check=False)
        self.assertEqual(0, result.returncode, result.stderr)
        return json.loads(result.stdout)

    def test_policy_blocks_inline_handlers_and_loads_external_application_in_order(self):
        nodes = Elements(self.html).nodes
        policy = next(attrs["content"] for tag, attrs in nodes
                      if tag == "meta" and attrs.get("http-equiv") == "Content-Security-Policy")
        directives = {part.split()[0]: part.split()[1:]
                      for part in policy.split(";") if part.strip()}
        self.assertEqual(["'none'"], directives["base-uri"])
        self.assertEqual(["'none'"], directives["object-src"])
        self.assertEqual(["'none'"], directives["script-src-attr"])
        # Cloudflare's dynamic nonce must arrive in an HTTP header; a second
        # static script-src policy here would block its injected bot script.
        self.assertNotIn("script-src", directives)
        scripts = [attrs for tag, attrs in nodes if tag == "script"]
        self.assertEqual(["site-data-loader.js", "app.js"], [x["src"] for x in scripts[:2]])
        self.assertTrue(all(x.get("src") for x in scripts))
        self.assertIsNone(re.search(r"\bon[a-z]+\s*=", self.html + self.code))

    def test_dataset_strings_cannot_create_executable_handler_attributes(self):
        result = self.js('''
            const payload = "');globalThis.auditExecuted=true;//";
            console.log(JSON.stringify({
              security: holdingIdentityCells({cusip: payload, issuer: '<img src=x onerror="bad()">'}),
              holder: holderFundCell({cik: '\\" onmouseover=\\"bad()', name: '<script>bad()</script>'}),
            }));
        ''')
        for markup in result.values():
            nodes = Elements(markup).nodes
            self.assertTrue(nodes)
            self.assertFalse(any(tag in {"script", "img"} for tag, _ in nodes))
            self.assertFalse(any(name.lower().startswith("on") for _, attrs in nodes for name in attrs))
            for tag, attrs in nodes:
                if tag == "a":
                    self.assertTrue(attrs["href"].startswith(("#stock/", "#fund/")))

    def test_sort_uses_native_button_and_announces_direction(self):
        result = self.js('''
            const attrs = {};
            const th = {dataset: {col: 'value'}, setAttribute:(k,v)=>attrs[k]=v,
              querySelector:()=>({textContent:''}), classList:{add:()=>{},remove:()=>{}}};
            global.document={getElementById:()=>({querySelectorAll:()=>[th]})};
            updateSortArrows('fundTable',{col:'value',dir:'asc'});
            console.log(JSON.stringify({html:sortableHeader('onFundSort','value','Value','right'),attrs}));
        ''')
        self.assertTrue(any(tag == "button" for tag, _ in Elements(result["html"]).nodes))
        self.assertEqual("ascending", result["attrs"]["aria-sort"])

    def test_malformed_percent_encoded_route_recovers_without_throwing(self):
        result = self.js('''
            let home=0;
            global.location={hash:'#stock/%E0%A4%A'};
            goHome=()=>{home++};
            routeFromHash();
            console.log(JSON.stringify({home}));
        ''')
        self.assertEqual(1, result["home"])

    def test_search_inputs_have_persistent_accessible_names(self):
        markup = self.html + self.code
        for identifier in ["gsearch", "homeSearch", "fundHoldingsSearch", "stockHoldersSearch"]:
            attrs = next(attrs for tag, attrs in Elements(markup).nodes
                         if tag == "input" and attrs.get("id") == identifier)
            self.assertTrue(attrs.get("aria-label"))


if __name__ == "__main__":
    unittest.main()
