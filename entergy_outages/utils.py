from datetime import datetime
from pathlib import Path
import hashlib
import json

import git
from tqdm import tqdm


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


def insert_from_file(file_versions, db):
    """recieves a generator of file versions and inserts them into a database"""
    for i, (commit_time, commit_hash, file_contents) in enumerate(file_versions):
        try:
            # quick and dirty way to check if the file is valid json
            outages = json.loads(file_contents)
        except ValueError:
            continue

        for outage in outages:
            # create a unique id for each outage
            outage_id = outage["zip"] + outage["_loaded_at"]
            outage_id = hashlib.md5(outage_id.encode()).hexdigest()
        
            model = {
                "id": outage_id,
                "zip": outage["zip"],
                "state": outage["state"],
                "customers_served": outage["customersServed"],
                "customers_affected": outage["customersAffected"],
                "last_updated_time": outage["lastUpdatedTime"],
                "latitude": outage["latitude"],
                "longitude": outage["longitude"],
                "_loaded_at": outage["_loaded_at"],
                "_commit_time": commit_time.isoformat(timespec="auto"),
                "_commit_hash": commit_hash,
                "_version": i,
            }

            db["outages"].insert(
                model, 
                pk="id",
                columns={
                    # remeber to use sqlite types
                    "id": "text",
                    "zip": "text",
                    "state": "text",
                    "customers_served": "integer",
                    "customers_affected": "integer",
                    "last_updated_time": "text",
                    "latitude": "float",
                    "longitude": "float",
                    "_loaded_at": "text",
                    "_commit_time": "text",
                    "_commit_hash": "text",
                    "_version": "text",
                }
            )

        print(f"Inserted {len(outages)} outages from {commit_hash}")
        

    
