#!/usr/bin/env python3
"""
Rebuild a unified folder structure from multiple backup archives without loading
all file contents into RAM.

This version is designed to work even when the output directory is on a
removable drive or Windows-like filesystem (exFAT/NTFS/FAT) that rejects some
valid archive names, such as names ending with a space or a dot.

Key additions compared to the original script:
- Accepts both archive files and directories containing archives.
- Skips the output directory if it appears in the input glob.
- Portable-name mode (enabled by default): rewrites path components that are not
  representable on Windows-like filesystems in a reversible way.
  Example:
      "LGA "   -> "LGA%20"
      "Bločky " -> "Bločky%20"
      "pondelok 8.7." -> "pondelok 8.7%2E"
  The CSV log preserves the original path.
- Continues reconstructing instead of failing on those entries.

Supported input formats:
- .zip
- .tar
- .tar.gz
- .tgz
- .tar.bz2
- .tar.xz
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path, PurePosixPath
import posixpath
import shutil
import sys
import tarfile
import zipfile
from typing import BinaryIO, Iterable, Iterator, Optional

BUFFER_SIZE = 8 * 1024 * 1024  # 8 MiB
IGNORE_NAMES = {
    ".DS_Store",
    "Thumbs.db",
}
IGNORE_DIRS = {
    "__MACOSX",
}
WINDOWS_INVALID_CHARS = set('<>:"\\|?*')
WINDOWS_RESERVED_BASENAMES = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}


class Stats:
    def __init__(self) -> None:
        self.archives = 0
        self.entries_seen = 0
        self.files_written = 0
        self.files_skipped_identical = 0
        self.files_renamed = 0
        self.dirs_created = 0
        self.paths_sanitized = 0
        self.skipped_unsupported = 0
        self.skipped_special = 0
        self.skipped_unsafe = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Merge multiple backup archives into one reconstructed directory tree."
    )
    p.add_argument(
        "archives",
        nargs="+",
        help=(
            "Input archives and/or directories containing archives "
            "(.zip, .tar, .tar.gz, .tgz, .tar.bz2, .tar.xz)"
        ),
    )
    p.add_argument(
        "--output-dir",
        required=True,
        help="Directory where the merged structure will be reconstructed",
    )
    p.add_argument(
        "--output-archive",
        help="Optional final archive to create from the reconstructed directory (.zip, .tar.gz, .tgz, .tar)",
    )
    p.add_argument(
        "--log-csv",
        default="migration_log.csv",
        help="CSV file for extraction/migration log (default: migration_log.csv in current directory)",
    )
    p.add_argument(
        "--strip-components",
        type=int,
        default=0,
        help="Strip N leading path components from paths inside archives",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite conflicting files instead of preserving both versions",
    )
    p.add_argument(
        "--no-pack",
        action="store_true",
        help="Do not create final archive even if --output-archive is given",
    )
    portability = p.add_mutually_exclusive_group()
    portability.add_argument(
        "--portable-names",
        dest="portable_names",
        action="store_true",
        default=True,
        help=(
            "Rewrite path components that are invalid on removable/Windows-like filesystems "
            "(default: enabled)"
        ),
    )
    portability.add_argument(
        "--exact-names",
        dest="portable_names",
        action="store_false",
        help="Keep exact archive names. Use only on Linux-native filesystems.",
    )
    return p.parse_args()


def is_supported_archive(path: Path) -> bool:
    name = path.name.lower()
    return (
        name.endswith(".zip")
        or name.endswith(".tar")
        or name.endswith(".tar.gz")
        or name.endswith(".tgz")
        or name.endswith(".tar.bz2")
        or name.endswith(".tar.xz")
    )


def is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def iter_archives_from_inputs(inputs: Iterable[str], out_dir: Path, output_archive: Optional[Path]) -> Iterator[Path]:
    seen: set[Path] = set()

    for raw in inputs:
        candidate = Path(raw).expanduser()
        resolved = candidate.resolve(strict=False)

        if not candidate.exists():
            yield resolved
            continue

        if candidate.is_dir():
            for sub in sorted(candidate.rglob("*")):
                sub_resolved = sub.resolve(strict=False)
                if output_archive is not None and sub_resolved == output_archive:
                    continue
                if is_relative_to(sub_resolved, out_dir):
                    continue
                if sub.is_file() and is_supported_archive(sub):
                    if sub_resolved not in seen:
                        seen.add(sub_resolved)
                        yield sub_resolved
        else:
            if output_archive is not None and resolved == output_archive:
                continue
            if is_relative_to(resolved, out_dir):
                continue
            if resolved not in seen:
                seen.add(resolved)
                yield resolved


def encode_for_portable_component(component: str) -> str:
    """
    Make a single path component representable on Windows-like filesystems.

    Rules:
    - '%' is escaped to '%25' to keep transformations reversible.
    - characters invalid on Windows-like filesystems become '%HH'.
    - trailing spaces and dots become '%20' / '%2E'.
    - reserved basenames (CON, AUX, COM1, ...) get a '%5F' suffix before the extension.
    """
    if component == "":
        return component

    component = component.replace("%", "%25")

    trim_at = len(component)
    while trim_at > 0 and component[trim_at - 1] in (" ", "."):
        trim_at -= 1

    core = component[:trim_at]
    trailing = component[trim_at:]

    if core == "" and trailing:
        core = "_"

    out: list[str] = []
    for ch in core:
        code = ord(ch)
        if code < 32 or ch in WINDOWS_INVALID_CHARS:
            out.append(f"%{code:02X}")
        else:
            out.append(ch)

    for ch in trailing:
        out.append(f"%{ord(ch):02X}")

    sanitized = "".join(out)

    if sanitized in ("", ".", ".."):
        sanitized = sanitized.replace(".", "%2E") or "_"

    # Windows reserved names are checked on the basename without extension.
    if "." in sanitized:
        stem, suffix = sanitized.split(".", 1)
        if stem.upper() in WINDOWS_RESERVED_BASENAMES:
            sanitized = f"{stem}%5F.{suffix}"
    else:
        if sanitized.upper() in WINDOWS_RESERVED_BASENAMES:
            sanitized = f"{sanitized}%5F"

    return sanitized


def sanitize_member_path(raw_path: str, strip_components: int, portable_names: bool) -> tuple[Optional[Path], bool]:
    # Convert backslashes to slashes for ZIP-created Windows paths.
    raw_path = raw_path.replace("\\", "/")
    raw_path = posixpath.normpath(raw_path)

    if raw_path in ("", ".", "/"):
        return None, False

    parts = [p for p in PurePosixPath(raw_path).parts if p not in ("", "/", ".")]

    if len(parts) <= strip_components:
        return None, False

    parts = parts[strip_components:]

    if not parts:
        return None, False

    # Refuse unsafe traversal.
    if any(part == ".." for part in parts):
        return None, False

    if parts[0] in IGNORE_DIRS:
        return None, False
    if parts[-1] in IGNORE_NAMES:
        return None, False

    changed = False
    final_parts: list[str] = []
    for part in parts:
        if portable_names:
            encoded = encode_for_portable_component(part)
            if encoded != part:
                changed = True
            final_parts.append(encoded)
        else:
            final_parts.append(part)

    return Path(*final_parts), changed


def mkdir_if_missing(path: Path, stats: Stats) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        stats.dirs_created += 1


def ensure_parent_dir(path: Path, stats: Stats) -> None:
    mkdir_if_missing(path.parent, stats)


def streams_identical(existing_path: Path, src_stream: BinaryIO, expected_size: Optional[int]) -> bool:
    """
    Compare an existing file with a source stream chunk by chunk without loading
    everything into memory. Consumes src_stream to EOF.
    """
    if expected_size is not None and existing_path.stat().st_size != expected_size:
        while src_stream.read(BUFFER_SIZE):
            pass
        return False

    with existing_path.open("rb") as existing:
        while True:
            a = existing.read(BUFFER_SIZE)
            b = src_stream.read(BUFFER_SIZE)
            if a != b:
                return False
            if not a and not b:
                return True


def unique_conflict_path(dest: Path, archive_stem: str) -> Path:
    safe_stem = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in archive_stem).strip("_")
    safe_stem = safe_stem or "archive"

    parent = dest.parent
    stem = dest.stem
    suffix = dest.suffix

    candidate = parent / f"{stem}__from_{safe_stem}{suffix}"
    counter = 2
    while candidate.exists():
        candidate = parent / f"{stem}__from_{safe_stem}__dup{counter}{suffix}"
        counter += 1
    return candidate


class LogWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        mkdir_if_missing(path.parent, Stats())
        self.fh = path.open("w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.fh)
        self.writer.writerow([
            "archive",
            "original_path",
            "final_path",
            "action",
            "note",
        ])

    def write(self, archive: str, original_path: str, final_path: str, action: str, note: str = "") -> None:
        self.writer.writerow([archive, original_path, final_path, action, note])

    def close(self) -> None:
        self.fh.close()


def copy_stream_to_path(src_stream: BinaryIO, dest_path: Path) -> None:
    with dest_path.open("wb") as out:
        shutil.copyfileobj(src_stream, out, length=BUFFER_SIZE)


def write_file_from_stream(
    src_stream: BinaryIO,
    dest: Path,
    archive_path: Path,
    original_member_path: str,
    expected_size: Optional[int],
    overwrite: bool,
    stats: Stats,
    log: LogWriter,
    note: str = "",
) -> None:
    ensure_parent_dir(dest, stats)

    if dest.exists() and dest.is_dir():
        conflict_dest = unique_conflict_path(dest.with_suffix(""), archive_path.stem)
        ensure_parent_dir(conflict_dest, stats)
        copy_stream_to_path(src_stream, conflict_dest)
        stats.files_written += 1
        stats.files_renamed += 1
        log.write(str(archive_path), original_member_path, str(conflict_dest), "renamed_conflict", note or "path occupied by directory")
        return

    if not dest.exists():
        copy_stream_to_path(src_stream, dest)
        stats.files_written += 1
        log.write(str(archive_path), original_member_path, str(dest), "written", note)
        return

    if overwrite:
        copy_stream_to_path(src_stream, dest)
        stats.files_written += 1
        log.write(str(archive_path), original_member_path, str(dest), "overwritten", note)
        return

    if streams_identical(dest, src_stream, expected_size):
        stats.files_skipped_identical += 1
        log.write(str(archive_path), original_member_path, str(dest), "skipped_identical", note)
        return

    raise RuntimeError("Source stream was consumed during comparison; caller must reopen and retry.")


def process_zip(
    archive_path: Path,
    out_dir: Path,
    strip_components: int,
    overwrite: bool,
    portable_names: bool,
    stats: Stats,
    log: LogWriter,
) -> None:
    with zipfile.ZipFile(archive_path, "r") as zf:
        infos = zf.infolist()
        for idx, info in enumerate(infos, start=1):
            stats.entries_seen += 1
            rel, rel_changed = sanitize_member_path(info.filename, strip_components, portable_names)
            if rel is None:
                stats.skipped_unsafe += 1
                log.write(str(archive_path), info.filename, "", "skipped", "unsafe/ignored/empty path")
                continue
            if rel_changed:
                stats.paths_sanitized += 1

            dest = out_dir / rel
            note = "sanitized_for_portable_fs" if rel_changed else ""

            if info.is_dir():
                mkdir_if_missing(dest, stats)
                log.write(str(archive_path), info.filename, str(dest), "dir", note)
                continue

            with zf.open(info, "r") as src:
                if not dest.exists() or overwrite:
                    write_file_from_stream(
                        src_stream=src,
                        dest=dest,
                        archive_path=archive_path,
                        original_member_path=info.filename,
                        expected_size=info.file_size,
                        overwrite=overwrite,
                        stats=stats,
                        log=log,
                        note=note,
                    )
                else:
                    identical = streams_identical(dest, src, info.file_size)
                    if identical:
                        stats.files_skipped_identical += 1
                        log.write(str(archive_path), info.filename, str(dest), "skipped_identical", note)
                    else:
                        conflict_dest = unique_conflict_path(dest, archive_path.stem)
                        ensure_parent_dir(conflict_dest, stats)
                        with zf.open(info, "r") as src2:
                            copy_stream_to_path(src2, conflict_dest)
                        stats.files_written += 1
                        stats.files_renamed += 1
                        log.write(str(archive_path), info.filename, str(conflict_dest), "renamed_conflict", note)

            if idx % 500 == 0:
                print(f"  {archive_path.name}: processed {idx}/{len(infos)} entries...", flush=True)


def process_tar(
    archive_path: Path,
    out_dir: Path,
    strip_components: int,
    overwrite: bool,
    portable_names: bool,
    stats: Stats,
    log: LogWriter,
) -> None:
    with tarfile.open(archive_path, "r:*") as tf:
        for idx, member in enumerate(tf, start=1):
            stats.entries_seen += 1
            rel, rel_changed = sanitize_member_path(member.name, strip_components, portable_names)
            if rel is None:
                stats.skipped_unsafe += 1
                log.write(str(archive_path), member.name, "", "skipped", "unsafe/ignored/empty path")
                continue
            if rel_changed:
                stats.paths_sanitized += 1

            dest = out_dir / rel
            note = "sanitized_for_portable_fs" if rel_changed else ""

            if member.isdir():
                mkdir_if_missing(dest, stats)
                log.write(str(archive_path), member.name, str(dest), "dir", note)
                continue

            if member.issym() or member.islnk() or member.ischr() or member.isblk() or member.isfifo():
                stats.skipped_special += 1
                log.write(str(archive_path), member.name, "", "skipped", f"special entry type: {member.type!r}")
                continue

            extracted = tf.extractfile(member)
            if extracted is None:
                stats.skipped_special += 1
                log.write(str(archive_path), member.name, "", "skipped", "cannot extract member")
                continue

            with extracted as src:
                if not dest.exists() or overwrite:
                    write_file_from_stream(
                        src_stream=src,
                        dest=dest,
                        archive_path=archive_path,
                        original_member_path=member.name,
                        expected_size=member.size,
                        overwrite=overwrite,
                        stats=stats,
                        log=log,
                        note=note,
                    )
                else:
                    identical = streams_identical(dest, src, member.size)
                    if identical:
                        stats.files_skipped_identical += 1
                        log.write(str(archive_path), member.name, str(dest), "skipped_identical", note)
                    else:
                        conflict_dest = unique_conflict_path(dest, archive_path.stem)
                        ensure_parent_dir(conflict_dest, stats)
                        extracted2 = tf.extractfile(member)
                        if extracted2 is None:
                            stats.skipped_special += 1
                            log.write(str(archive_path), member.name, "", "skipped", "cannot reopen member after conflict")
                        else:
                            with extracted2 as src2:
                                copy_stream_to_path(src2, conflict_dest)
                            stats.files_written += 1
                            stats.files_renamed += 1
                            log.write(str(archive_path), member.name, str(conflict_dest), "renamed_conflict", note)

            if idx % 500 == 0:
                print(f"  {archive_path.name}: processed {idx} entries...", flush=True)


def pack_output_dir(out_dir: Path, output_archive: Path) -> None:
    fmt = output_archive.name.lower()

    if fmt.endswith(".zip"):
        base = str(output_archive.with_suffix(""))
        shutil.make_archive(base, "zip", root_dir=out_dir.parent, base_dir=out_dir.name)
    elif fmt.endswith(".tar.gz") or fmt.endswith(".tgz"):
        if fmt.endswith(".tgz"):
            base = str(output_archive.with_suffix(""))
            shutil.make_archive(base, "gztar", root_dir=out_dir.parent, base_dir=out_dir.name)
            created = Path(base + ".tar.gz")
            if created.exists():
                created.rename(output_archive)
        else:
            base = str(output_archive)[:-7]  # strip .tar.gz
            shutil.make_archive(base, "gztar", root_dir=out_dir.parent, base_dir=out_dir.name)
    elif fmt.endswith(".tar"):
        base = str(output_archive)[:-4]
        shutil.make_archive(base, "tar", root_dir=out_dir.parent, base_dir=out_dir.name)
    else:
        raise ValueError("Unsupported output archive format. Use .zip, .tar.gz, .tgz, or .tar")


def main() -> int:
    args = parse_args()

    out_dir = Path(args.output_dir).expanduser().resolve()
    log_csv = Path(args.log_csv).expanduser().resolve()
    output_archive = Path(args.output_archive).expanduser().resolve() if args.output_archive else None

    archives = list(iter_archives_from_inputs(args.archives, out_dir=out_dir, output_archive=output_archive))

    stats = Stats()
    out_dir.mkdir(parents=True, exist_ok=True)
    log = LogWriter(log_csv)

    try:
        for archive in archives:
            if not archive.exists():
                print(f"[WARN] Archive not found: {archive}", file=sys.stderr)
                stats.skipped_unsupported += 1
                log.write(str(archive), "", "", "skipped", "archive not found")
                continue
            if not is_supported_archive(archive):
                print(f"[WARN] Unsupported archive type, skipped: {archive}", file=sys.stderr)
                stats.skipped_unsupported += 1
                log.write(str(archive), "", "", "skipped", "unsupported archive type")
                continue

            stats.archives += 1
            print(f"[INFO] Processing: {archive}", flush=True)
            name = archive.name.lower()
            try:
                if name.endswith(".zip"):
                    process_zip(
                        archive,
                        out_dir,
                        args.strip_components,
                        args.overwrite,
                        args.portable_names,
                        stats,
                        log,
                    )
                else:
                    process_tar(
                        archive,
                        out_dir,
                        args.strip_components,
                        args.overwrite,
                        args.portable_names,
                        stats,
                        log,
                    )
            except Exception as exc:
                print(f"[ERROR] Failed to process {archive}: {exc}", file=sys.stderr)
                log.write(str(archive), "", "", "error", str(exc))

        if output_archive and not args.no_pack:
            print(f"[INFO] Packing merged directory into: {output_archive}", flush=True)
            pack_output_dir(out_dir, output_archive)

    finally:
        log.close()

    print("\n=== DONE ===")
    print(f"Processed archives      : {stats.archives}")
    print(f"Entries seen           : {stats.entries_seen}")
    print(f"Files written          : {stats.files_written}")
    print(f"Identical files skipped: {stats.files_skipped_identical}")
    print(f"Conflicts renamed      : {stats.files_renamed}")
    print(f"Directories created    : {stats.dirs_created}")
    print(f"Paths sanitized        : {stats.paths_sanitized}")
    print(f"Unsupported skipped    : {stats.skipped_unsupported}")
    print(f"Special entries skipped: {stats.skipped_special}")
    print(f"Unsafe/ignored skipped : {stats.skipped_unsafe}")
    print(f"Merged directory       : {out_dir}")
    print(f"Migration log          : {log_csv}")
    if output_archive and not args.no_pack:
        print(f"Final archive          : {output_archive}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
