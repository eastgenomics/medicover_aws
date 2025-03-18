import json

import polars as pl


def parse_json(json_file: str):
    """Parse a JSON file

    Parameters
    ----------
    json_file : str
        Path to the JSON file

    Returns
    -------
    dict
        Dict containing the JSON data
    """

    with open(json_file) as f:
        data = json.loads(f.read())

    return data


def parse_xlsx(xlsx_file: str):
    df = pl.read_excel(xlsx_file)
    df = df.with_columns(Panels=pl.col("Panels").str.split(";"))
    return df


def parse_tsv(tsv_file: str):
    data = []

    with open(tsv_file) as f:
        for line in f:
            panel_id, name, relevant_disorders = line.strip().split("\t")
            data.append(
                {
                    "id": panel_id,
                    "name": name,
                    "relevant_disorders": eval(relevant_disorders),
                }
            )

    return data


def get_evaluations(json_data: dict):
    """Get evaluation data from a given Medicover report

    Parameters
    ----------
    json_data : dict
        Dict resulting from parsing the JSON report file

    Returns
    -------
    dict
        Dict containing the evaluation data
    """

    return json_data[2]["data"]["evaluations"]


def add_missing_keys(data):
    """Add unique keys in all dicts contained in data as SQLAlchemy can't bulk
    insert otherwise

    Parameters
    ----------
    data : list
        List of dicts

    Returns
    -------
    list
        List of dicts with all possible keys
    """

    new_data = []
    all_keys = set()

    # gather all unique keys
    for data_dict in data:
        for key in data_dict:
            all_keys.add(key)

    for data_dict in data:
        for key in all_keys:
            # if the key is not present in the given dict, add it
            if key not in data_dict:
                data_dict[key] = "[null]"

        new_data.append(data_dict)

    return new_data
