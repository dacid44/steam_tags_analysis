import sys
import os
import json
import pickle
import gzip
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from apis import APIs

apis = APIs()

DISCORD_URL = apis.discord_url
DISCORD_MESSAGE = "### Fetching games with index {}-{} ({} games). Currently fetching game {}/{} {}{}"
DISCORD_ERROR = "\n:warning: Error on appid {}:\n```json\n{}\n```"

start = int(sys.argv[1])
end = int(sys.argv[2])
filename = sys.argv[3]
rate = float(sys.argv[4]) if len(sys.argv) >= 5 else 1.55

if filename.endswith(".gz"):
    use_gzip = True
elif filename.endswith(".json"):
    use_gzip = False
else:
    print("Filename should end in .gz or .json")
    sys.exit(1)

appids = pd.read_csv("top10000games.csv", index_col=0)["appid"].tolist()[start:end]
num_games = len(appids)

errors = {}
message_id = None

executor = ThreadPoolExecutor(max_workers=1)

def send_discord_message(edit, index, complete):
    global message_id
    try:
        discord_request = {
            "content": DISCORD_MESSAGE.format(
                start,
                end,
                num_games,
                index,
                num_games,
                ":white_check_mark:" if complete else ":repeat:",
                ''.join(DISCORD_ERROR.format(appid, error) for appid, error in errors.items()),
            ),
        }
        if edit:
            requests.patch(
                DISCORD_URL + f"/messages/{message_id}",
                json=discord_request,
            )
        else:
            response = requests.post(DISCORD_URL, params={ "wait": True }, json=discord_request)
            message_id = int(response.json()["id"])
    except Exception as e:
        print("Error sending discord webhook request:", e)

def check_response(index, appid, response):
    response_json = response.json()
    if response_json[str(appid)]["success"]:
        success = True
    else:
        errors[int(appid)] = response_json[str(appid)]
        success = False
    if index % 2 == 1 or index == num_games - 1:
        executor.submit(send_discord_message, True, index + 1, index == num_games - 1)
    return success

send_discord_message(False, 0, False)

print(f"Fetching games with index {start}-{end} ({num_games} games)")
data = {
    int(appid): response.json()[str(appid)]["data"]
    for i, (appid, response) in tqdm(enumerate(apis.fetch_games_ratelimited(appids, rate)), total=num_games)
    if check_response(i, appid, response)
}
print(f"Successfully fetched {len(data)} of {num_games} games")

if use_gzip:
    print(f"Saving gzipped JSON data to {filename}")
    with gzip.open(filename, "wt") as f:
        json.dump(data, f)
else:
    print(f"Saving JSON data to {filename}")
    with open(filename, "wt") as f:
        json.dump(data, f)

if errors:
    name, ext = os.path.splitext(filename)
    if ext == ".gz":
        name, _ = os.path.splitext(name)
    error_file_name = name + ".error.json"
    print(f"Found {len(errors)} errors, saving to {error_file_name}")
    with open(error_file_name, "wt") as f:
        json.dump(errors, f)