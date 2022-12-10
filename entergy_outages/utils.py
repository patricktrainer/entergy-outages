from datetime import datetime

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
        except git.GitCommandError:
            pass
