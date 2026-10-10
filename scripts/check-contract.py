#!/usr/bin/env python3
"""Verify the bundled contract; optionally compare the canonical product source."""
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--main-repo", type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "contract/snapshot.json").read_text())

def hashes(directory):
    return {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(directory.rglob("*")) if p.is_file()}

if hashes(root / "contract/rust") != manifest["files"]:
    raise SystemExit("Bundled contract differs from its recorded snapshot")
if args.main_repo and hashes(args.main_repo / manifest["source_path"]) != manifest["files"]:
    raise SystemExit("Product contract changed: review and update the snapshot together")
print("Contract snapshot matches" + (" the product source" if args.main_repo else " its recorded hashes"))
