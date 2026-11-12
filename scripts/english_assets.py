"""Apply the documented English-only text patch to an ECharts 5.6.0 bundle."""

import argparse
from pathlib import Path
import re


NOTICE = "/* Modified for Purchase Pulse: English display strings; see ../licenses/SOURCES.md. */\n"
HAN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002fa1f\U00030000-\U000323af]")
GEOGRAPHIC_LABELS = {
    "\u5357\u6d77\u8bf8\u5c9b": "South China Sea Islands",
    "\u5e7f\u4e1c": "Guangdong",
    "\u9999\u6e2f": "Hong Kong",
    "\u6fb3\u95e8": "Macau",
    "\u5929\u6d25": "Tianjin",
    "\u53f0\u6e7e": "Taiwan",
}


def locale_bounds(source, marker):
    start = source.index(marker) + len(marker)
    if source[start] != "{":
        raise ValueError("Unsupported ECharts locale structure")
    depth = 0
    quote = None
    escaped = False
    for index in range(start, len(source)):
        character = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
        elif character in ('"', "'"):
            quote = character
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return start, index + 1
    raise ValueError("Unterminated ECharts locale object")


def english_bundle(source):
    if 't.version="5.6.0"' not in source:
        raise ValueError("Expected the official ECharts 5.6.0 distribution")
    source = source.removeprefix(NOTICE)
    english_start, english_end = locale_bounds(source, "zc(Pc,")
    english = source[english_start:english_end]
    alternate_start, alternate_end = locale_bounds(source, "zc(Lc,")
    source = source[:alternate_start] + english + source[alternate_end:]
    for original, translated in GEOGRAPHIC_LABELS.items():
        source = source.replace('"' + original + '"', '"' + translated + '"')
    # Preserve the internal font-width probe without a literal Han character.
    source = source.replace('"\u56fd"', '"\\u56fd"')
    if HAN.search(source):
        raise ValueError("Unrecognized non-English text remains in the bundle")
    return NOTICE + source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Official ECharts 5.6.0 dist/echarts.min.js")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = english_bundle(args.input.read_text(encoding="utf-8"))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(result, encoding="utf-8")
    print(f"Wrote English chart bundle: {args.output}")


if __name__ == "__main__":
    main()
