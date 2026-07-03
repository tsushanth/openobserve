# Test Setup Contract: Logs No-Stream Quick Pick (area: Logs)

## Streams / data the spec must establish
Tag each item by SCOPE so the Engineer puts it in the right place:
- **`[shared/read-only]`** — every test just READS it the same way → set up ONCE / use a pre-seeded stream.
- **`[per-test]`** — only one test needs it, or a test MUTATES it → set up INSIDE that test, uniquely named.

### `e2e_automate` (stream) **[shared/read-only]**
- Fields: `level`, `job`, `log`, `_timestamp`, `code`, `stream`, `kubernetes_host`, `kubernetes_container_name`, `kubernetes_container_hash`, etc. (standard e2e test stream)
- Why: The quick pick relies on stream list being populated from `getStreams` API. This pre-existing stream ensures the sidebar quick pick has at least one item to show. All tests just read this list; none modify the stream itself.
- **Already seeded** by the global test data ingestion step in most Logs test suites. If running standalone, ingest once:
  ```
  await ingestTestData(page, "e2e_automate");
  ```
  See: `tests/ui-testing/playwright-tests/utils/data-ingestion.js:11`

## How to create it (copy these EXACT patterns — do NOT invent setup)

### Navigate to logs page (without stream pre-selected)
```js
const logData = require("../../fixtures/log.json");
const { getOrgIdentifier } = require('../utils/cloud-auth.js');

await page.goto(`${logData.logsUrl}?org_identifier=${getOrgIdentifier()}`);
```
See: `tests/ui-testing/playwright-tests/Logs/logspage.spec.js:60`

### Ingest test data (ensures stream exists)
```js
const { ingestTestData } = require('../utils/data-ingestion.js');
await ingestTestData(page, "e2e_automate");
```
See: `tests/ui-testing/playwright-tests/Logs/ftsDefaultColumn.spec.js:23,37`

### Auth / org
- ORGNAME = `default` (from env `ORGNAME`; use `getOrgIdentifier()`)
- Auth state is already established by `navigateToBase(page)` in the enhanced base fixtures.
  See: `tests/ui-testing/playwright-tests/utils/enhanced-baseFixtures.js`

### Page Manager initialization
```js
const PageManager = require('../../pages/page-manager.js');
const pm = new PageManager(page);
```
See: `tests/ui-testing/playwright-tests/Logs/logspage.spec.js:3,49`

### Timing: wait for stream list to load
After navigating to logs page, the stream list arrives asynchronously via `getStreamList()` →
`getStreams()` API. Wait for the quick pick area to become visible:
```js
await page.locator('[data-test="logs-search-stream-quick-pick"]').waitFor({ state: 'visible', timeout: 15000 });
```

## Preconditions / toggles
- `auto_query_enabled` config may affect whether `recentStreams` (via localStorage) appear in the LogsNoStreamState "Recent:" chips. Quick pick in the sidebar (IndexList) does NOT depend on this config.
- Ensure SQL mode is NOT active — quick pick is only visible when no stream is selected (it's in the no-stream empty state).
- Stream list must be non-empty. The `e2e_automate` stream + any other pre-seeded streams suffice.

## Gotchas (so the Healer/Engineer don't rediscover them)
- The quick pick buttons `[data-test="logs-search-stream-quick-pick-<streamName>"]` are rendered
  **only inside the sidebar IndexList** (left panel), NOT in the main content area LogsNoStreamState.
- The main content LogsNoStreamState (hero state with "Select a stream" and "Recent:" chips) is at
  `[data-test="logs-search-no-stream-selected-text"]` in Index.vue. It's a separate component from
  the sidebar quick pick.
- `quickPickStreams` is sorted by `doc_time_max` from `streamResults.list`. If API returns stats
  without `doc_time_max`, the fallback uses plain `streamList` order. The quick pick buttons use
  stream names as their `data-test` suffix — sanitize dots/hyphens before constructing selectors.
- The sidebar OSelect dropdown (`[data-test="log-search-index-list-select-stream"]`) is also
  visible in the no-stream state. Tests that click the "Select a stream" card in the hero state
  will trigger `onSelectStream()` which opens this dropdown.
- When a quick pick button is clicked, `quickSelectStream()` calls `handleStreamSelection()` which
  sets `selectedStream`, then calls `onStreamChange("")` which triggers field loading. The fields
  arrive async — wait for the field list to appear before asserting stream selection completed.
- If `auto_query_enabled` is false AND the user hasn't previously selected a stream,
  `recentStreams` in LogsNoStreamState will be empty and the chips won't render. This is
  expected behavior, not a bug.
- `streamResults.list` contains the full stream objects (with `stats`, `schema`, etc.) and is
  populated AFTER `getStreamList()` completes. Before that, `quickPickStreams` falls back to
  the plain `streamList` computed property (which is set earlier from the same data). Wait
  for `streamList.length > 0` before proceeding.
