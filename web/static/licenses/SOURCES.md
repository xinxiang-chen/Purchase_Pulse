# Dashboard map and chart assets

## US state boundaries

`us-states.geojson` derives from the official Apache ECharts example asset:
https://echarts.apache.org/examples/data/asset/geo/USA.json

Repository copy:
https://github.com/apache/echarts-examples/blob/gh-pages/public/data/asset/geo/USA.json

The upstream examples repository is distributed under Apache License 2.0; its license is included as `MAP-LICENSE`. The original JSON has no embedded separate license notice.

Changes: remove District of Columbia and Puerto Rico, retaining exactly the 50 states including Alaska and Hawaii; add `properties.code` using USPS postal abbreviations; round coordinates to four decimal places while preserving all retained upstream boundary vertices and FIPS feature IDs. The source is already a simplified map (about 88 KB); no boundaries were invented and no additional point-removal simplification was applied.

USPS abbreviation reference: https://pe.usps.com/text/pub28/pub28apb.htm

`properties.name` and `properties.code` match the 50-state `config/us_states.json` catalog exactly.

The source uses unwrapped west longitudes down to about -187 degrees for Alaska. These are preserved so the original ECharts geometry is not distorted across the antimeridian. Use ECharts `specialAreas` for Alaska and Hawaii insets; do not stretch the full geographic extent into a single lower-48 view.

## Apache ECharts 5.6.0

`echarts.min.js` is based on the official release-tag distribution:
https://raw.githubusercontent.com/apache/echarts/5.6.0/dist/echarts.min.js

Included upstream notices:
- `ECHARTS-LICENSE`: Apache License 2.0, plus subcomponent information.
- `ECHARTS-NOTICE`: Apache Software Foundation notice.
- `ECHARTS-LICENSE-d3`: BSD 3-Clause terms referenced by the ECharts license for embedded d3 components.

Full source URLs, file sizes, transformation details, and SHA-256 checksums are recorded in `asset-manifest.json`.

## Local English display-text changes

The chart bundle is modified for this English-language dashboard:

- The alternate locale registration uses the existing upstream English locale object, including dates, toolbox labels, chart names, and accessibility text.
- Embedded geographic display names use English equivalents.
- The internal font-width probe uses a JavaScript Unicode escape instead of a literal Han character. Its measured character and layout behavior are unchanged.
- A modification notice is added before the original distribution header. All upstream license and notice files remain unchanged.

The transformation is reproducible with the repository's `scripts/english_assets.py`:

```bash
.venv/bin/python scripts/english_assets.py /path/to/upstream/echarts.min.js \
  --output web/static/js/echarts.min.js
```

Use the official ECharts 5.6.0 input linked above. Rebuilding the asset is optional for normal application startup; the modified bundle is already included. The manifest records both the original distribution checksum and the modified file checksum.
