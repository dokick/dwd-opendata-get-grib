"""
This module is for my bachelor so now shut up
"""

import json
import os.path


def optimize_json(path: str, file_name: str):
    """Optimize json format from DWD server

    Args:
        path (str): path to json file, excluding file name
        file_name (str): file name
    """

    with open(os.path.join(path, file_name), "r", encoding="utf8") as json_file:
        json_dict = json.load(json_file)
        amount_of_messages = len(json_dict["messages"][0])
        json_obj = json_dict["messages"][0]
        new_json = {json_obj[i]["key"]: json_obj[i]["value"] for i in range(amount_of_messages)}

    name = file_name[:-5]
    end = file_name[-5:]

    with open(os.path.join(path, fr"{name}_new{end}"), "w", encoding="utf8") as new_json_file:
        json.dump(new_json, new_json_file, indent=4)


def main():
    """For testing and debugging purposes"""
    path = os.path.join(os.path.expanduser("~"),
                        "Dokumente", "_TU", "Bachelor", "DWD", "icon-d2", "00", "u")
    file_name = r"icon-d2_germany_icosahedral_model-level_2022110300_000_10_u.json"
    optimize_json(path, file_name)


if __name__ == "__main__":
    main()
