import sys
import os
import json
import gzip
import typing
from typing import cast
from tqdm import tqdm

JSONObject = dict[str, "JSONData"]
JSONData = (
    int | float | bool | str | list["JSONData"] | tuple["JSONData", ...] | JSONObject
)


def open_maybe_gz(filename: str, mode: str) -> typing.IO:
    if os.path.splitext(filename)[1] == ".gz":
        return cast(typing.IO, gzip.open(filename, mode))
    else:
        return open(filename, mode)


def read_json_gz(filename: str) -> JSONObject:
    with open_maybe_gz(filename, "rt") as f:
        return json.load(f)


def write_json_gz(filename: str, obj: JSONData):
    with open_maybe_gz(filename, "wt") as f:
        json.dump(obj, f)


def combine_json(files: list[str]) -> JSONObject:
    data = {}
    for file in files:
        data.update(read_json_gz(file))
    return data


def main():
    match sys.argv[1:]:
        case ["help" | "--help"]:
            print("""\
Subcommands:
    combine [input-files] <output_file>: Combine multiple JSON objects into one.
    extract-keys [input-files] <output-file>: Extract the (unique) keys from one or more JSON objects or lists into one list.
    reorder <steamspy-file> <input-file> <output-file>: Reorder the items in the input according to the order of the appids column from the speamspy csv file.
    prepare-data <steamspy-file> <steam-file> <output-file> <num-rows>: Prepare tags and descriptions of the given data, and truncate to the given number of rows.""")

        case ["combine", *input_files, output_file]:
            data: JSONObject = {}
            for file in tqdm(input_files):
                data.update(read_json_gz(file))

            write_json_gz(output_file, data)

        case ["extract-keys", *input_files, output_file]:
            keys = set()
            for file in tqdm(input_files):
                data = read_json_gz(file)
                if isinstance(data, dict):
                    keys.update(data.keys())
                else:
                    keys.update(data)

            write_json_gz(output_file, list(keys))

        case ["reorder", steamspy_file, input_file, output_file]:
            import pandas as pd

            keys = [
                str(key)
                for key in pd.read_csv(steamspy_file, index_col=0)["appid"].tolist()
            ]
            data = read_json_gz(input_file)
            reordered_data: JSONObject = {}
            for key in tqdm(keys):
                if key in data:
                    reordered_data[key] = data[key]
                    # In a probably false hope to minimize memory usage
                    del data[key]

            write_json_gz(output_file, reordered_data)

        case [
            "prepare-data",
            steamspy_file,
            steam_file,
            output_file,
            num_rows,
        ] if num_rows.isdigit():
            import pandas as pd
            from bs4 import BeautifulSoup

            # Load steamspy data
            print("Reading SteamSpy data")
            with open_maybe_gz(steamspy_file, "rt") as f:
                df_steamspy = pd.read_json(f, orient="index")[
                    ["name", "owners", "languages", "tags"]
                ]
            df_steamspy = cast(
                pd.DataFrame,
                df_steamspy[
                    cast(pd.Series, df_steamspy["languages"])
                    .fillna("")
                    .str.startswith("English")
                ],
            )

            def tag_categories(row: pd.Series):
                tags = cast(dict[str, int], row["tags"])
                # Filter out games with no tags
                if not tags:
                    return None
                total = sum(tags.values())
                # Return "relative importance" of each tag to the game
                return pd.Series(
                    {
                        "tag " + category.lower(): votes / total
                        for category, votes in tags.items()
                    }
                )

            print("Processing tags")
            df_tags = df_steamspy.apply(tag_categories, axis=1).fillna(0)
            df_tags = cast(pd.DataFrame, df_tags[df_tags.sum(axis=1) > 0])

            print("Reading Steam data")
            steam_data = read_json_gz(steam_file)

            def fetch_description(appid, key: str) -> str:
                appid = str(appid)
                if appid not in steam_data:
                    return ""
                description = cast(str, cast(JSONObject, steam_data[appid])[key] or "")
                description = BeautifulSoup(description, "html.parser").get_text(" ", strip=True)
                return description.strip()

            print("Matching descriptions")
            df_tags["short_description"] = (
                df_tags.index.to_series()
                .map(lambda x: fetch_description(x, "short_description"))
                .fillna("")
            )
            df_tags["detailed_description"] = (
                df_tags.index.to_series()
                .map(lambda x: fetch_description(x, "detailed_description"))
                .fillna("")
            )
            df_tags["about_the_game"] = (
                df_tags.index.to_series()
                .map(lambda x: fetch_description(x, "about_the_game"))
                .fillna("")
            )
            df_tags = df_tags[
                (df_tags["detailed_description"] != "")
                & (df_tags["short_description"] != "")
                & (df_tags["about_the_game"] != "")
            ]

            print("Joining dataframes")
            df = (
                df_steamspy[["name", "owners"]]
                .join(df_tags, how="inner")
                .head(int(num_rows))
            )

            print("Fixing owners column")
            df["owners_min"] = df["owners"].map(
                lambda x: int(x.split(" .. ")[0].replace(",", ""))
            )
            df["owners_max"] = df["owners"].map(
                lambda x: int(x.split(" .. ")[1].replace(",", ""))
            )
            df.drop(columns=["owners"], inplace=True)

            print("Writing output file")
            with open_maybe_gz(output_file, "wt") as f:
                df.to_csv(f)

        case _:
            raise Exception("Invalid arguments")


if __name__ == "__main__":
    main()
