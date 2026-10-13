# Ponytail review: OCR automatic language selection (master..feat/ocr-auto-language)

Ordered by lines saved.

- `AutoLanguageOCRParser.java:L69,71,75,93-96` + `Extractor.java:L424,566`: yagni: live `BooleanSupplier enabled` exists only so a `setOcrLanguage` call after construction turns routing off; nothing in extract or Datashare does that (`configure` decides before installing the router). Drop the supplier, the pass-through branch, `autoOcrLanguage = false` in `setOcrLanguage`, and tests `test_setting_a_language_after_construction_skips_routing` + `test_a_disabled_router_passes_straight_through`. Behavior change: a late `setOcrLanguage` no longer disables an installed router. About -32 lines.
- `ExtractorAutoLanguageTest.java:L146`: delete: `test_missing_models_keep_plain_ocr_and_warn_once` calls `withAutoLanguage` once, so it can't check "once"; the probed-once test already covers it. Move its `isSameAs(EmptyParser.INSTANCE)` and `contains("script/HanS")` asserts there. About -25 lines.
- `AutoLanguageOCRParser.java:L180` + `Tess4JOCRParser.java:L467` + `Tess4JOCRParserTest.java:L344`: delete: per-read `SKIP_CONFIDENCE=false` guards against a parser-level `skipConfidence=true` no production path sets (only test helpers call `setSkipConfidence`). Delete the line, restore `getSkipConfidence()` at L467 (it has no caller left now), delete `test_per_call_config_turns_confidence_back_on`. About -21 lines.
- `Extractor.java:L288-292,464` + `ExtractorAutoLanguageTest.java:L139`: delete: 0 to 100 clamp, its warning, the getter and its test. Confidence is always 0 to 100, so 150 behaves like 100 and -5 like 0. `ifPresent(n -> this.ocrRetryConfidence = n)`, 1 line. About -13 lines.
- `AutoLanguageOCRParserTest.java:L380,407-409`: delete: `failOcr` duplicates `failingModel`. Set `failingModel = "script/HanS+script/Latin"` at L213. About -4 lines.
- `Extractor.java:L420`: delete: `autoOcrLanguage = false` after a missing-models result; the static cache already answers the next call (after an empty probe, a second `setOcrConfig` on the same Extractor would probe again). -1 line.

net: -96 lines possible.
