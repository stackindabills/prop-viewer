"""
Fetch NBA player prop markets and produce FanDuel-only outputs.

Key features:
- Requires DRAFT_ODDS_API_KEY via environment variable
- Fetches NBA player props from The Odds API v4
- Filters FanDuel only
- Keeps Over/Under only
- Recomputes NO-VIG PROBABILITY correctly per Over/Under PAIR
- Writes:
    - fanduel_player_props_basketball_nba_YYYYMMDD.csv
    - cleaned_props.csv (stable filename for GitHub Pages)
"""

import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from collections import defaultdict

import requests


# ================== ENV ==================

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}\n"
            f"Set it locally or in GitHub Secrets."
        )
    return value


API_KEY = require_env("DRAFT_ODDS_API_KEY")

BASE = os.getenv("DRAFT_ODDS_BASE_URL", "https://api.the-odds-api.com")
API_PREFIX = "/v4"
DEFAULT_TIMEOUT = (5, 15)
OUTPUT_DIR = os.getcwd()

PLAYER_MARKETS = [
    "player_points_rebounds_assists",
    "player_points",
    "player_assists",
    "player_threes",
]


# ================== HELPERS ==================

def _url(path: str) -> str:
    return f"{BASE}{API_PREFIX}{path}"


def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "odds-player-props-client/1.0",
    })
    return s


def _american_to_prob(american: Optional[float]) -> Optional[float]:
    if american is None:
        return None
    try:
        a = float(american)
    except Exception:
        return None
    if a > 0:
        return 100.0 / (a + 100.0)
    return (-a) / ((-a) + 100.0)


# ================== FETCH ==================

def fetch_nba_events(session: requests.Session) -> List[Dict[str, Any]]:
    url = _url("/sports/basketball_nba/events")
    params = {"apiKey": API_KEY, "regions": "us", "dateFormat": "iso"}
    r = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_event_player_props(
    session: requests.Session, event_id: str, markets: List[str]
) -> Dict[str, Any]:
    url = _url(f"/sports/basketball_nba/events/{event_id}/odds")
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": ",".join(markets),
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    r = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()


# ================== FLATTEN ==================

def flatten_events_to_rows(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for event in events:
        if not event:
            continue

        event_id = event.get("id")
        home = event.get("home_team")
        away = event.get("away_team")
        commence = event.get("commence_time")

        for bookmaker in event.get("bookmakers", []) or []:
            for market in bookmaker.get("markets", []) or []:
                for o in market.get("outcomes", []) or []:
                    rows.append({
                        "event_id": event_id,
                        "home_team": home,
                        "away_team": away,
                        "commence_time": commence,
                        "bookmaker": bookmaker.get("key"),
                        "bookmaker_title": bookmaker.get("title"),
                        "market": market.get("key"),
                        "outcome": o.get("name"),
                        "description": o.get("description", ""),
                        "point": o.get("point"),
                        "price_american": o.get("price"),
                        "implied_prob": round(
                            _american_to_prob(o.get("price")) or 0.0, 6
                        ),
                        # placeholder – fixed later
                        "no_vig_prob": None,
                    })

    return rows


# ================== FILTER ==================

def filter_fanduel_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    wanted = set(PLAYER_MARKETS)
    out = []

    for r in rows:
        if (r.get("bookmaker") or "").lower() != "fanduel":
            continue
        if r.get("market") not in wanted:
            continue
        if (r.get("outcome") or "").lower() not in ("over", "under"):
            continue
        out.append(r)

    return out


# ================== NO-VIG (CORRECT) ==================

def add_no_vig_over_under(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Compute no-vig probability ONLY within each Over/Under pair:
      (event_id, market, description, point)
    """

    groups = defaultdict(list)

    for r in rows:
        key = (
            r.get("event_id"),
            r.get("market"),
            r.get("description"),
            r.get("point"),
        )
        groups[key].append(r)

    for items in groups.values():
        ou = [r for r in items if r["outcome"].lower() in ("over", "under")]
        if len(ou) != 2:
            continue

        implied = []
        for r in ou:
            p = _american_to_prob(r["price_american"])
            implied.append(p if p is not None else 0.0)

        total = sum(implied)
        if total <= 0:
            continue

        for r, p in zip(ou, implied):
            r["no_vig_prob"] = round(p / total, 6)

    return rows


# ================== OUTPUT ==================

def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        open(path, "w", encoding="utf-8").close()
        return

    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


# ================== MAIN ==================

def main() -> int:
    session = create_session()
    print("Fetching NBA events...")

    events = fetch_nba_events(session)

    all_rows: List[Dict[str, Any]] = []

    for ev in events:
        eid = ev.get("id")
        if not eid:
            continue
        try:
            resp = fetch_event_player_props(session, eid, PLAYER_MARKETS)
            all_rows.extend(flatten_events_to_rows([resp]))
            print(f"Processed event {eid}")
        except Exception as e:
            print(f"Skipping event {eid}: {e}")

    # FanDuel + Over/Under only
    fanduel_rows = filter_fanduel_rows(all_rows)

    # FIXED no-vig calculation
    fanduel_rows = add_no_vig_over_under(fanduel_rows)

    today = datetime.utcnow().strftime("%Y%m%d")

    dated_csv = os.path.join(
        OUTPUT_DIR, f"fanduel_player_props_basketball_nba_{today}.csv"
    )
    stable_csv = os.path.join(OUTPUT_DIR, "cleaned_props.csv")

    write_csv(dated_csv, fanduel_rows)
    write_csv(stable_csv, fanduel_rows)

    print(f"Wrote {dated_csv}")
    print(f"Wrote {stable_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
