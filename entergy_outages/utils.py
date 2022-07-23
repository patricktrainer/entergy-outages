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

