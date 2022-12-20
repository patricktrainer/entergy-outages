from datetime import datetime
from pathlib import Path
import hashlib
import json

import git
import sqlite_utils


def from_epoch(epoch_time: int) -> str:
    """
    Convert epoch time to a datetime object
    """
    # convert to string and strip last three characters (milliseconds)
    epoch_time = int(str(epoch_time)[:-3])
    formatted = datetime.fromtimestamp(epoch_time).isoformat(timespec="auto")
    return formatted


def today() -> str:
    """
    Get today's date in ISO format
    """
    return datetime.utcnow().isoformat(timespec="auto")


def iterate_file_versions(repo_path, filepath, ref="main"):
    """
    Iterate through the versions of a file in a git repo
    """
    repo = git.Repo(repo_path, odbt=git.GitCmdObjectDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepath)))

    for commit in commits:
        commit_tree = commit.tree
        blobs = commit_tree.blobs

        try:
            blob = [b for b in blobs if b.name == Path(filepath).name][0]
            yield commit.committed_datetime, commit.hexsha, blob.data_stream.read()
        except Exception:
            continue


def insert_from_file(file_versions, db) -> None:
    for i, (commit_time, commit_hash, file_contents) in enumerate(file_versions):
        try:
            outages = json.loads(file_contents)
        except ValueError:
            continue

        for outage in outages:
            # create a unique id for each outage
            outage_id = outage["zip"] + outage["_loaded_at"]
            outage_id = hashlib.md5(outage_id.encode()).hexdigest()
            db["outages"].insert(
                dict(outage, id=outage_id),
                pk="id",
                alter=True,
                replace=True,
            )
