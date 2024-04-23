import sys
import json
import pickle
import gzip
import pandas as pd
from tqdm import tqdm
from apis import APIs

apis = APIs()

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

print(f"Fetching games with index {start}-{end} ({end - start} games)")
data = {
    int(appid): response.json()[str(appid)]["data"]
    for appid, response in tqdm(apis.fetch_games_ratelimited(appids, rate), total=end - start)
}
print(f"Successfully fetched {len(data)} of {end - start} games")

if use_gzip:
    print(f"Saving gzipped JSON data to {filename}")
    with gzip.open(filename, "wt") as f:
        json.dump(data, f)
else:
    print(f"Saving JSON data to {filename}")
    with open(filename, "wt") as f:
        json.dump(data, f)