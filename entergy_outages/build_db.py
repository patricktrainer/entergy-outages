import utils
import sqlite_utils


if __name__ == "__main__":

    ref = "main"
    filepath = "entergy_outages_zipcode.json"
    file_versions = utils.iterate_file_versions(".", filepath, ref=ref)
    db = sqlite_utils.Database("entergy_outages.db", recreate=True)

    utils.insert_from_file(file_versions, db)

