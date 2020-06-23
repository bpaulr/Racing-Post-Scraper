import sqlite3
from copy import deepcopy
from typing import Dict, Union
from typing import List
from typing import Tuple

import requests
from racingpost_scraper.pipelines import DATABASE
from racingpost_scraper.pipelines import sort_dict

TABLE = "Sires"


def gen_sire_table_statement(fields: List[str], primary_keys: List[str]) -> str:
    ccols = primary_keys.copy()
    for field in fields:
        if field not in primary_keys:
            ccols.append(field)
    cols = ', '.join([field + " TEXT" for field in ccols])
    pks = ', '.join(primary_keys)
    create_statement = f"""CREATE TABLE IF NOT EXISTS {TABLE} ({cols}, PRIMARY KEY ({pks}))"""
    return create_statement


def gen_sire_insert_statement(stats: Dict[str, str]) -> str:
    keys, values = [], []
    for k, v in stats.items():
        keys.append(k)
        values.append("?")
    keys_str = ', '.join(keys)
    values_str = ', '.join(values)
    return f"INSERT INTO {TABLE} ({keys_str}) VALUES ({values_str})"


def generate_sire_table(horses: List[Tuple[str, str]]) -> None:
    """
    Create a table of sires that inclues their progeny statistics.
    :param horses: List of tuples of the form (horse_name, profile_url)
    :return: None
    """
    connection = sqlite3.connect(DATABASE)
    cursor = connection.cursor()

    first_iteration = False

    for horse_name, profile_url in horses:
        prog_data = get_horse_progeny(profile_url)
        if prog_data is None:
            print(f"There seems to be no progeny data for {horse_name}, {profile_url}.")
            continue
        for stats in prog_data:
            stats_copy = deepcopy(stats)
            stats_copy["horse_name"] = horse_name
            sorted_stats = sort_dict(stats_copy)

            if not first_iteration:
                first_iteration = True
                create_statement = gen_sire_table_statement(list(sorted_stats.keys()), ["horse_name", "category"])
                cursor.execute(create_statement)
                connection.commit()

            insert_statement = gen_sire_insert_statement(sorted_stats)
            try:
                cursor.execute(insert_statement, [str(stat) for stat in sorted_stats.values()])
            except sqlite3.IntegrityError:
                pass

    connection.commit()
    connection.close()


def get_horse_progeny(profile_url: str, progency_suffix: str = "progeny-statistics", api_str: str = "tab") -> Union[None, Dict[str, str]]:
    name_url = requests.get(profile_url).url
    prog_url = name_url + "/" + progency_suffix
    full_url = prog_url.replace("profile/horse", "profile/{}/horse").format(api_str)
    response = requests.get(full_url)
    json_data = response.json()

    if json_data is None or not json_data:
        return None

    idx1 = None
    for key in json_data.keys():
        idx1 = key

    idx2 = None
    for key in json_data[idx1].keys():
        idx2 = key
    return response.json()[idx1][idx2]


def main():
    horses = [
        ("society-rock", "https://www.racingpost.com/profile/horse/738816/society-rock"),
    ]
    generate_sire_table(horses)


if __name__ == '__main__':
    main()
