import json
import requests
import utils

def get(url: str) -> json:
    """
    Get data from the API
    """
    response = requests.get(url)
    return response.json()


def enrich(data: json) -> json:
    """
    Add additional keys to the data
    """
    for item in data:
        item['lastUpdatedTime'] = utils.from_epoch(item['lastUpdatedTime'])
        item["_loaded_at"] = utils.today()
    return json.dumps(data, indent=4)


def to_file(filename: str, data: json) -> None:
    """
    Write the data to a file
    """
    # add json file extension if not provided
    if not filename.endswith(".json"):
        filename += ".json"
    with open(filename, "w") as f:
        f.write(data)


if __name__ == "__main__":
    entergy_data = {
        "county": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyNOLA/county",
        "zipcode": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyNOLA/zip",
    }

    for key, value in entergy_data.items():
        data = get(value)
        data = enrich(data)
        to_file(f"entergy_outages_{key}", data)
        print(f"Wrote {key} to file")

    print("Done.")
