import os
import requests
from requests.exceptions import HTTPError
from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from tqdm import tqdm
import time

STEAM_API_BASE = "https://api.steampowered.com/"
STEAM_STORE_API_BASE = "https://store.steampowered.com/api/"
STEAMSPY_API_URL = "https://steamspy.com/api.php"

load_dotenv()

class APIs:
    steam_api_key = os.environ['STEAM_API_KEY']
    discord_url = os.environ["DISCORD_WEBHOOK_URL"]
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
    
    def fetch_games_ratelimited(self, appids: [int], rate: float) -> [(int, requests.Response)]:
        t = time.perf_counter() + rate
        for appid in appids:
            response = self.fetch_game(appid)
            response.raise_for_status()
            time.sleep(max(t - time.perf_counter(), 0))
            t = time.perf_counter() + rate
            yield (appid, response)
    
    async def fetch_games_async(self, appids: [int]):
        games = {}
        async with aiohttp.ClientSession() as session:
            for appid in tqdm(appids):
                async with session.get(STEAM_STORE_API_BASE + "appdetails", params={ "appids": str(appid) }) as response:
                    response.raise_for_status()
                    games.update(await response.json())
        return games

    def steamspy_request(self, request: str, appid: int | None = None, page: int | None = None):
        params = {
            "request": request,
        }
        if appid is not None:
            params["appid"] = str(appid)
        if page is not None:
            params["page"] = str(page)

        response = requests.get(STEAMSPY_API_URL, params)
        response.raise_for_status()
        return response
        apps = { int(appid): data for appid, data in response.json().items() }
        for appid in apps:
            owner_range = [int(num.replace(",", "").strip()) for num in apps[appid]["owners"].split("..")]
            apps[appid]["owners"] = tuple(owner_range)
        return apps
    
    async def steamspy_async(self, appids: [int], progressbar=None):
        if progressbar is None:
            progressbar = lambda x: x
        async with aiohttp.ClientSession() as session:
            for appid in progressbar(appids):
                params = {
                    "request": "appdetails",
                    "appid": appid
                }
                async with session.get(STEAMSPY_API_URL, params=params) as response:
                    response.raise_for_status()
                    yield (appid, await response.json())
    
    def compile_steamspy_all_pages(self, start: int, end: int) -> (pd.DataFrame, [int]):
        games = {}
        failed = []
        for i in range(start, end + 1):
            try:
                games.update(self.steamspy_request("all", page=i))
            except HTTPError as e:
                failed.append(i)
        
        return (pd.DataFrame.from_dict(games, orient="index"), failed)
