#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import tarfile
import zipfile
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Dict, Iterable, Iterator, List, Set


SKIP_NAMES = {
    ".DS_Store",
    "Thumbs.db",
}
SKIP_PREFIXES = (
    "__MACOSX/",
)


@dataclass
class Node:
    dirs: Dict[str, "Node"] = field(default_factory=dict)
    files: Set[str] = field(default_factory=set)

    def add_dir(self, parts: List[str]) -> None:
        current = self
        for part in parts:
            current = current.dirs.setdefault(part, Node())

    def add_file(self, parts: List[str]) -> None:
        if not parts:
            return
        *dir_parts, file_name = parts
        current = self
        for part in dir_parts:
            current = current.dirs.setdefault(part, Node())
        current.files.add(file_name)


def normalize_path(path: str, drop_leading: int = 0) -> List[str]:
    path = path.replace("\\", "/").strip()
    if not path:
        return []

    for prefix in SKIP_PREFIXES:
        if path.startswith(prefix):
            return []

    pure = PurePosixPath(path)
    parts = [p for p in pure.parts if p not in ("", ".")]

    if drop_leading > 0:
        parts = parts[drop_leading:]

    if parts and parts[-1] in SKIP_NAMES:
        return []

    return parts


def iter_zip_entries(path: str) -> Iterator[tuple[str, bool]]:
    with zipfile.ZipFile(path, "r") as zf:
        for info in zf.infolist():
            yield info.filename, info.is_dir()


def iter_tar_entries(path: str) -> Iterator[tuple[str, bool]]:
    with tarfile.open(path, "r:*") as tf:
        for member in tf:
            yield member.name, member.isdir()


def iter_archive_entries(path: str) -> Iterator[tuple[str, bool]]:
    lower = path.lower()
    if zipfile.is_zipfile(path):
        yield from iter_zip_entries(path)
        return

    tar_like_exts = (
        ".tar",
        ".tar.gz",
        ".tgz",
        ".tar.bz2",
        ".tbz2",
        ".tar.xz",
        ".txz",
    )
    if lower.endswith(tar_like_exts):
        yield from iter_tar_entries(path)
        return

    # Fallback: try tar if extension check did not catch it.
    try:
        yield from iter_tar_entries(path)
        return
    except tarfile.ReadError:
        pass

    raise ValueError(
        f"Unsupported archive format: {path}\n"
        "Supported formats out of the box: .zip, .tar, .tar.gz, .tgz, .tar.bz2, .tar.xz"
    )


def merge_archives(archive_paths: List[str], drop_leading: int = 0, verbose: bool = True) -> Node:
    root = Node()

    for archive_path in archive_paths:
        if verbose:
            print(f"[INFO] Reading: {archive_path}", file=sys.stderr)

        entry_count = 0
        for raw_path, is_dir in iter_archive_entries(archive_path):
            entry_count += 1
            parts = normalize_path(raw_path, drop_leading=drop_leading)
            if not parts:
                continue

            if is_dir or raw_path.endswith(("/", "\\")):
                root.add_dir(parts)
            else:
                root.add_file(parts)

        if verbose:
            print(f"[INFO] Entries processed: {entry_count}", file=sys.stderr)

    return root


def render_tree(node: Node, prefix: str = "") -> List[str]:
    lines: List[str] = []

    dir_names = sorted(node.dirs.keys(), key=str.casefold)
    file_names = sorted(node.files, key=str.casefold)
    items = [(name, True) for name in dir_names] + [(name, False) for name in file_names]

    for index, (name, is_dir) in enumerate(items):
        is_last = index == len(items) - 1
        connector = "└── " if is_last else "├── "
        lines.append(prefix + connector + name)

        if is_dir:
            child_prefix = prefix + ("    " if is_last else "│   ")
            lines.extend(render_tree(node.dirs[name], child_prefix))

    return lines


def count_nodes(node: Node) -> tuple[int, int]:
    dir_count = len(node.dirs)
    file_count = len(node.files)
    for child in node.dirs.values():
        child_dirs, child_files = count_nodes(child)
        dir_count += child_dirs
        file_count += child_files
    return dir_count, file_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Merge folder/file structures from multiple archives into a single text tree "
            "without extracting them to disk."
        )
    )
    parser.add_argument(
        "archives",
        nargs="+",
        help="Paths to archives (.zip, .tar, .tar.gz, .tgz, .tar.bz2, .tar.xz)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="merged_tree.txt",
        help="Output text file path (default: merged_tree.txt)",
    )
    parser.add_argument(
        "--drop-leading",
        type=int,
        default=0,
        help=(
            "Drop the first N path components from every entry. Useful if all backups start with "
            "something like 'Takeout/Drive/'. Example: --drop-leading 2"
        ),
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Do not write the summary header at the top of the output file",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress messages to stderr",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    missing = [p for p in args.archives if not os.path.exists(p)]
    if missing:
        for p in missing:
            print(f"[ERROR] File not found: {p}", file=sys.stderr)
        return 1

    try:
        root = merge_archives(args.archives, drop_leading=args.drop_leading, verbose=not args.quiet)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    lines: List[str] = []
    if not args.no_summary:
        dir_count, file_count = count_nodes(root)
        lines.append("Merged archive tree")
        lines.append(f"Archives: {len(args.archives)}")
        lines.append(f"Directories: {dir_count}")
        lines.append(f"Files: {file_count}")
        lines.append("")

    lines.extend(render_tree(root))

    with open(args.output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    if not args.quiet:
        print(f"[INFO] Done. Output written to: {args.output}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
