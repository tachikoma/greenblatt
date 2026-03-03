from __future__ import annotations

import argparse
from pathlib import Path


def parse_env_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    if not path.exists():
        return keys

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue

        key = line.split("=", 1)[0].strip()
        if key:
            keys.add(key)
    return keys


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare keys between .env and .env.sample")
    parser.add_argument("--env", default=".env", help="Path to runtime env file")
    parser.add_argument("--sample", default=".env.sample", help="Path to sample env file")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return exit code 1 when differences exist",
    )
    args = parser.parse_args()

    env_path = Path(args.env)
    sample_path = Path(args.sample)

    env_keys = parse_env_keys(env_path)
    sample_keys = parse_env_keys(sample_path)

    only_in_env = sorted(env_keys - sample_keys)
    only_in_sample = sorted(sample_keys - env_keys)

    print(f"[ENV-CHECK] env={env_path} keys={len(env_keys)}")
    print(f"[ENV-CHECK] sample={sample_path} keys={len(sample_keys)}")

    if not only_in_env and not only_in_sample:
        print("[ENV-CHECK] OK: key sets are identical")
        return 0

    if only_in_env:
        print("[ENV-CHECK] Keys only in .env:")
        for key in only_in_env:
            print(f"  - {key}")

    if only_in_sample:
        print("[ENV-CHECK] Keys only in .env.sample:")
        for key in only_in_sample:
            print(f"  - {key}")

    if args.strict:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
