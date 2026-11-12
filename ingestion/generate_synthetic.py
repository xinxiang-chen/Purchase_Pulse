"""Generate reproducible fictional US shopping purchases for the dashboard."""

import argparse
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import random
from tempfile import NamedTemporaryFile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIELDS = ["event_id", "user_id", "age", "state", "amount", "event_time"]
AGE_RANGES = [(18, 24), (25, 34), (35, 44), (45, 54), (55, 80)]


def load_states(path):
    states = json.loads(path.read_text(encoding="utf-8"))
    codes = [entry["code"] for entry in states]
    if (
        len(codes) != 50
        or len(set(codes)) != 50
        or "DC" in codes
        or not {"AK", "HI"}.issubset(codes)
        or any(len(code) != 2 or not code.isascii() or not code.isupper() for code in codes)
    ):
        raise ValueError("State catalog must contain exactly 50 unique two-letter US state codes")
    return codes


def generate_rows(count, seed, states):
    """Use stable user profiles; the first 50 purchases cover every state."""
    rng = random.Random(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    user_count = min(500, max(50, count // 5))
    profiles = []
    for index in range(user_count):
        lower, upper = AGE_RANGES[index % len(AGE_RANGES)]
        profiles.append(
            {
                "user_id": f"demo-user-{index + 1:05d}",
                "age": rng.randint(lower, upper),
                "state": states[index] if index < len(states) else rng.choice(states),
            }
        )
    for index in range(count):
        profile = profiles[index] if index < len(profiles) else rng.choice(profiles)
        lower, upper = rng.choices(
            [(500, 4999), (5000, 14999), (15000, 49999)], weights=[6, 3, 1], k=1
        )[0]
        cents = rng.randint(lower, upper)
        yield {
            "event_id": f"demo-purchase-{seed}-{index + 1:08d}",
            "user_id": profile["user_id"],
            "age": profile["age"],
            "state": profile["state"],
            "amount": f"{Decimal(cents) / 100:.2f}",
            "event_time": (start + timedelta(milliseconds=50 * index))
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "data/user_log.csv")
    parser.add_argument("--force", action="store_true", help="Replace an existing output file.")
    args = parser.parse_args(argv)
    if args.rows < 1:
        parser.error("--rows must be positive")
    try:
        states = load_states(PROJECT_ROOT / "config/us_states.json")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(f"Cannot read the US state catalog: {exc}")
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not args.force:
        parser.error(f"{output} already exists; use --force to replace it")
    temporary = None
    try:
        with NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=output.parent,
                                prefix=f".{output.name}.", suffix=".tmp", delete=False) as csvfile:
            temporary = Path(csvfile.name)
            writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(generate_rows(args.rows, args.seed, states))
        if args.force:
            os.replace(temporary, output)
        else:
            # Same-directory hard link publishes the complete file without overwriting.
            os.link(temporary, output)
    except FileExistsError:
        parser.error(f"{output} already exists; use --force to replace it")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    print(f"Generated {args.rows} fictional US purchases: {output} (seed={args.seed})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
