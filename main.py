#!/bin/python
"""
Remove duplicates from a Google Photos takeout.
Replace album file photo copies with a metadata yaml file referencing the original files.
"""

from collections import defaultdict, namedtuple
import pathlib
import click
import re
from typing import Optional

import ruamel.yaml


Album = namedtuple(
    "Album", ["photo_files", "other_files", "name", "is_source", "is_special"]
)
Takeout = namedtuple("Takeout", ["photos_source", "albums", "special", "root_path"])

source_folder_regex = re.compile(r"^Photos from \d{4}$")
untitled_folder_regex = re.compile(r"^Untitled(?:\(\d+\))?$")

album_folder_name = "ALBUMS"
special_folders = ["Bin", "Archive", "Failed Videos", album_folder_name]
metadata_file_name = "metadata.json"


class FileCluster:
    """Bundle the actual image file with meta data file(s)."""

    def __init__(
        self, paths: list[pathlib.Path], gphotos_root_path: Optional[pathlib.Path]
    ):
        # first entry is the "base" file (e.g. photo.JPG)
        # use paths relative to the gphotos root path
        if gphotos_root_path is not None:
            self.paths = [
                p.relative_to(gphotos_root_path) if p.is_file() else p for p in paths
            ]
        else:
            self.paths = paths

    def __repr__(self):
        return f"FileCluster({', '.join(p.name for p in self.paths)})"

    def prefix_rename(self, prefix: str, task_ledger: list):
        """
        Rename all files in the cluster with the given prefix.
        """
        updated = []
        for path in self.paths:
            new_name = f"{prefix}{path.name}"
            new_path = path.parent / new_name
            task_ledger.append(("rename", path, new_path))
            updated.append(new_path)

        return FileCluster(paths=updated, gphotos_root_path=None)


def cluster_files_entries(entries: list[pathlib.Path], gphotos_root_path: pathlib.Path):
    # entries are sorted.
    # some metadata files have the full filename photo.JPG as base, others just the stem.
    current_cluster = None
    clusters = defaultdict(list)

    for entry in entries:
        if (
            current_cluster is None
            or not entry.name.startswith(current_cluster.name)
            or not entry.name.startswith(current_cluster.stem)
        ):
            current_cluster = entry

        clusters[current_cluster.name].append(entry)

    return {k: FileCluster(v, gphotos_root_path) for k, v in clusters.items()}


def index_folder(path: pathlib.Path, gphotos_root_path: pathlib.Path) -> Album:

    entries = sorted(list(path.iterdir()))

    is_source = source_folder_regex.match(path.name) is not None
    is_special = path.name in special_folders

    # assert no subfolders
    if any(p.is_dir() for p in entries):
        raise ValueError("Subfolders are not allowed in a album folder.")
    if not all(p.is_file() for p in entries):
        raise ValueError("All entries in the album folder must be files.")

    if is_source:
        # assert no metadata file
        if any(p.name == metadata_file_name for p in entries):
            raise ValueError("Metadata file is not allowed in a source folder.")

    clusters = cluster_files_entries(entries, gphotos_root_path)
    photo_files = {c: v for c, v in clusters.items() if c != metadata_file_name}
    other_files = {c: v for c, v in clusters.items() if c == metadata_file_name}

    album = Album(
        photo_files=photo_files,
        other_files=other_files,
        name=path.name,
        is_source=is_source,
        is_special=is_special,
    )
    return album


def open_gphotos_root_path(gphotos_root_path: pathlib.Path) -> Takeout:
    """ """
    albums = {}

    for image_dir in gphotos_root_path.iterdir():
        if not image_dir.is_dir():
            continue

        # if matches the regex, it is a source folder
        if source_folder_regex.match(image_dir.name):
            print(f"Found source folder: {image_dir.name}")
        else:
            print(f"Found non-source folder: {image_dir.name}")

        album = index_folder(image_dir, gphotos_root_path)
        # do not keep empty folders
        if not album.photo_files and not album.other_files:
            print(f"Empty folder: {image_dir.name}. Skipping.")
            continue
        albums[album.name] = album

    folder_special = {k: album for k, album in albums.items() if album.is_special}
    folder_source = {
        k: album
        for k, album in albums.items()
        if album.is_source and not album.is_special
    }
    folder_album = {
        k: album
        for k, album in albums.items()
        if not album.is_source and not album.is_special
    }

    if not folder_source:
        raise ValueError("Need at least one photo source folder.")

    return Takeout(
        photos_source=folder_source,
        albums=folder_album,
        special=folder_special,
        root_path=gphotos_root_path,
    )


def merge_files_in_albums(albums: dict[str, Album]) -> set:
    """
    Find the intersection of all photo files in the albums.
    """
    if not albums:
        return set()

    files = defaultdict(list)
    for album in albums.values():
        for file in album.photo_files:
            files[file].append(album.name)

    return files


def get_album_files_to_replace(album: Album, merged_source_files):
    # exactly once in source files
    replaceable_files = set()
    # not found in source files
    unique_files = set()
    # multiple times in source files
    unmatched_files = set()

    for file in album.photo_files:

        if file == metadata_file_name:
            continue

        if file not in merged_source_files:
            # print(f"  {file} is not in source files.")
            unique_files.add(file)
            continue

        source_files = merged_source_files[file]
        if len(source_files) > 1:
            # print(f"  {file} is in multiple source files: {source_files}")
            unmatched_files.add(file)
            continue

        source_file = source_files[0]
        replaceable_files.add(source_file)

    return replaceable_files, unique_files, unmatched_files


def compare_two_files(
    file1: pathlib.Path, file2: pathlib.Path, gphotos_root_path: pathlib.Path
) -> bool:
    """
    Compare two files and return True if they are the same.
    """
    # compare the contents of the files
    with open(gphotos_root_path / file1, "rb") as f1, open(
        gphotos_root_path / file2, "rb"
    ) as f2:
        return f1.read() == f2.read()


def _rename_cluster(
    photo_files: dict,
    prefix_str,
    duplicate_key,
    updated_key,
    task_ledger,
    album_files,
    source_files,
):
    """
    Rename the cluster with the given prefix.
    """
    cluster: FileCluster = photo_files[duplicate_key]
    renamed_cluster = cluster.prefix_rename(prefix_str, task_ledger)

    # make sure that none of the renamed files exists globally in the source files or album files
    if any(p.name in album_files for p in renamed_cluster.paths):
        raise ValueError(
            f"File {renamed_cluster.paths[0].name} already exists in album files."
        )
    if any(p.name in source_files for p in renamed_cluster.paths):
        raise ValueError(
            f"File {renamed_cluster.paths[0].name} already exists in source files."
        )

    photo_files[updated_key] = renamed_cluster
    photo_files.pop(duplicate_key)


def resolve_album_duplicate(
    takeout: Takeout, album_files, source_files, duplicate: str, task_ledger
) -> dict:
    """ """
    album_files_to_match: dict = {
        album: takeout.albums[album].photo_files[duplicate]
        for album in album_files[duplicate]
    }
    source_files_to_match: dict = {
        source: takeout.photos_source[source].photo_files[duplicate]
        for source in source_files[duplicate]
    }

    # sources key -> album list (if same file in multiple albums)
    matches = defaultdict(list)

    for ka, album_file in album_files_to_match.items():
        for ks, source_file in source_files_to_match.items():
            # compare only the cluster base file. Don't care about other files.
            if compare_two_files(
                album_file.paths[0], source_file.paths[0], takeout.root_path
            ):
                matches[ks].append(ka)
    # find unmatched source files
    unmatched_source = set(source_files_to_match.keys()) - set(matches.keys())

    rename_args = (task_ledger, album_files, source_files)

    for match_source, match_albums in matches.items():
        prefix_str = match_source.replace(" ", "_") + "__"
        updated_key = f"{prefix_str}{duplicate}"
        _rename_cluster(
            takeout.photos_source[match_source].photo_files,
            prefix_str,
            duplicate,
            updated_key,
            *rename_args,
        )

        for album in match_albums:
            _rename_cluster(
                takeout.albums[album].photo_files,
                prefix_str,
                duplicate,
                updated_key,
                *rename_args,
            )

    for unmatch in unmatched_source:
        # rename the source file with a prefix
        prefix_str = unmatch.replace(" ", "_") + "__"
        updated_key = f"{prefix_str}{duplicate}"
        _rename_cluster(
            takeout.photos_source[unmatch].photo_files,
            prefix_str,
            duplicate,
            updated_key,
            *rename_args,
        )

    # remove cluster from merged lists
    del album_files[duplicate]
    del source_files[duplicate]


def resolve_duplicates(takeout: Takeout, task_ledger):
    """
    Resolve duplicates in the source files and album files.
    """
    album_files = merge_files_in_albums(takeout.albums)
    source_files = merge_files_in_albums(takeout.photos_source)

    source_duplicates = {k: v for k, v in source_files.items() if len(v) > 1}
    for source_duplicate, sources in source_duplicates.items():
        resolve_album_duplicate(
            takeout, album_files, source_files, source_duplicate, task_ledger
        )


def replace_album_files_with_yaml_metadata(takeout: Takeout, task_ledger):
    """
    Replace album files with yaml metadata.
    """
    for album in takeout.albums.values():
        data = {
            "album": album.name,
            "photo_files": [
                path.name  # unique files, do not include album name
                for cluster in album.photo_files.values()
                for path in cluster.paths
            ],
        }

        yaml = ruamel.yaml.YAML()
        stream = ruamel.yaml.StringIO()
        yaml.dump(data, stream)
        yaml_str = stream.getvalue()

        yaml_path = pathlib.Path(album.name) / "album.yaml"
        task_ledger.append(("create", yaml_path, yaml_str))
        task_ledger.extend(
            [
                ("delete", path)
                for cluster in album.photo_files.values()
                for path in cluster.paths
            ]
        )


def remove_untitled_albums(takeout: Takeout, task_ledger):
    """
    Remove albums without a title.
    """
    to_delete = []
    for album in list(takeout.albums.values()):
        if untitled_folder_regex.fullmatch(album.name):

            # assert that all individual photo files are in the source folder
            all_available = True

            for filecluster_key in album.photo_files:
                for source in takeout.photos_source.values():
                    if filecluster_key in source.photo_files:
                        break
                else:
                    # file not found in any source folder
                    print(
                        f"File cluster {filecluster_key} not found in source folder. Not deleting untitled album {album.name}."
                    )
                    all_available = False
                    break

            if all_available:
                to_delete.append(album.name)

    for album_name in to_delete:
        task_ledger.append(("delete", pathlib.Path(album_name)))
        del takeout.albums[album_name]


def move_into_album_folder(
    takeout: Takeout, task_ledger, album_folder_name: str = album_folder_name
):
    """
    Move all albums into the ALBUMS folder.
    """
    album_folder = takeout.root_path / album_folder_name
    album_folder = album_folder.relative_to(takeout.root_path)
    task_ledger.append(("create_dir", album_folder))

    for album in takeout.albums.values():
        if not album.is_special:
            task_ledger.append(
                ("rename", pathlib.Path(album.name), album_folder / album.name)
            )


def optimize_takeout(takeout: Takeout, keep_untitled_albums: bool):

    task_ledger = []

    # take care of all duplicates in the source files
    resolve_duplicates(takeout, task_ledger)

    # remove untitled albums
    if not keep_untitled_albums:
        remove_untitled_albums(takeout, task_ledger)

    #
    replace_album_files_with_yaml_metadata(takeout, task_ledger)

    # Move all album folders into ALBUMS folder.
    move_into_album_folder(takeout, task_ledger)

    return task_ledger


def execute_task_ledger(gphotos_root_path: pathlib.Path, task_ledger, dry_run: bool):
    """
    Execute the tasks in the task ledger.
    """

    def delete(path: pathlib.Path):
        path = gphotos_root_path / path
        if path.is_dir():
            for child in path.iterdir():
                delete(child)
            path.rmdir()
        else:
            path.unlink()

    def create(path: pathlib.Path, content: str):
        path = gphotos_root_path / path
        with open(path, "w") as f:
            f.write(content)

    def create_dir(path: pathlib.Path):
        path = gphotos_root_path / path
        path.mkdir(parents=True, exist_ok=True)

    def rename(old_path: pathlib.Path, new_path: pathlib.Path):
        old_path = gphotos_root_path / old_path
        new_path = gphotos_root_path / new_path
        # this also works for non-empty directories
        old_path.rename(new_path)

    actions = {
        "delete": delete,
        "create": create,
        "create_dir": create_dir,
        "rename": rename,
    }

    for task in task_ledger:
        if dry_run:
            print(task)
        else:
            actions[task[0]](*task[1:])


@click.command()
@click.argument(
    "folder", type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path)
)
@click.option(
    "--keep-untitled-albums",
    is_flag=True,
    default=False,
    help="Keep albums without a title",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print actions without executing them",
)
def main(folder, keep_untitled_albums: bool, dry_run: bool):
    """
    Analyze the contents of the specified folder and print the counts of each file type.
    """
    takeout = open_gphotos_root_path(folder)
    task_ledger = optimize_takeout(takeout, keep_untitled_albums)
    execute_task_ledger(takeout.root_path, task_ledger, dry_run=dry_run)


if __name__ == "__main__":
    main()
