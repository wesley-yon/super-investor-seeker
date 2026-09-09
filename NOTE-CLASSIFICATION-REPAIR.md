# Reviewed note classification repair

The parser previously treated `IBONDS` fund names and preferred-share coupon
text as debt. This repair changes only the 59 exact NOTE identities in
`reviewed_note_classifications.json`: 57 ETF shares become EQUITY/ETF, and two
preferred depositary shares become PREF/PREFERRED. It does not implement the
broader preferred-share cleanup.

The checksummed review pairs exact SEC CUSIP/symbol observations with Nasdaq
exchange security descriptions and ETF flags. The source bytes and extracted
rows were checked against the recorded hashes. Existing reviewed ticker
spellings are retained. The correction adds no current quote permission and
does not promote the raw SEC resolver's unresolved records.

`note_classification.py` applies the same exact rule during parsing so later
filing ingestion does not recreate the error. Explicit and legacy options,
unreviewed debt identifiers, and conflicting reported CUSIPs are excluded.

The historical migration stages every changed fund before replacing files.
It verifies the prior composition hash, changes only approved NOTE types,
records the old hash and row changes, and recomputes the derived composition
hash. Filing text, source hashes, values, shares, and row counts stay intact.
Generated stock pages and histories regroup the repaired positions by their
correct type. Old CUSIP/NOTE links resolve to the corrected pages.

Both data workflows run the migration after restoring a nonlegacy snapshot.
A completion checksum is saved only after the registry and derived outputs
rebuild successfully. Later runs skip the completed migration; the ordinary
data validator rejects any remaining reviewed NOTE classification.

To inspect a restored snapshot locally:

```sh
python scripts/repair_note_classifications.py --workers 2 --report .cache/note-repair-dry-run.json
```

To apply and rebuild under the pipeline maintenance lock:

```sh
python scripts/repair_note_classifications.py --apply --rebuild --pending-only --workers 2 --report .cache/note-repair.json
python validate_data.py --incremental --refresh-cache
```

The September 8 local copy contained 23,176 affected rows in 6,002 quarters
across 1,796 funds. An independent comparison of all 9,506 fund files confirmed
unchanged source fields, amounts and row counts. The raw SEC master, source
state, and prior reviewed ticker map remained byte-identical. Local validation
does not establish that a production snapshot has been published.
