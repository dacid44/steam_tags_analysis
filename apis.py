import os
import requests
from dotenv import load_dotenv

STEAM_API_BASE = "https://api.steampowered.com/"
STEAM_STORE_API_BASE = "https://store.steampowered.com/api/"
STEAMSPY_API_URL = "https://steamspy.com/api.php"

load_dotenv()

class APIs:
    steam_api_key = os.environ['STEAM_API_KEY']
    def __init__(self) -> None:
        pass

    def fetch_all_games(self):
        response = requests.get(STEAM_API_BASE + "ISteamApps/GetAppList/v0002/", params={
            "key": self.steam_api_key,
            "format": "json",
        })
        return response.json()["applist"]["apps"]

    def fetch_game(self, appid: int):
        return requests.get(STEAM_STORE_API_BASE + "appdetails", params={
            "appids": str(appid),
        })

    def steamspy_request(self, request: str, appid: int | None = None, page: int | None = None):
        params = {
            "request": request,
        }
        if appid is not None:
            params["appid"] = str(appid)
        if page is not None:
            params["page"] = str(page)

        response = requests.get(STEAMSPY_API_URL, params)
        apps = { int(appid): data for appid, data in response.json().items() }
        for appid in apps:
            owner_range = [int(num.replace(",", "").strip()) for num in apps[appid]["owners"].split("..")]
            apps[appid]["owners"] = tuple(owner_range)
        return apps
