"""
Fetch NBA player prop markets and produce FanDuel-only outputs.

This script fetches per-event player prop odds from The Odds API v4, flattens
bookmakers/markets/outcomes into rows, filters for FanDuel bookmaker rows, and
writes both FanDuel-only JSON/CSV and a flattened JSON for reuse.

Also writes a stable CSV name `cleaned_props.csv` for GitHub Pages / viewer usage.

Usage:
  - Set DRAFT_ODDS_API_KEY in your environment (required).
  - Optionally set DRAFT_ODDS_BASE_URL.
  - Run: python scripts/build_csv.py (or wherever you place it)
"""

import csv
import glob
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests


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

# player prop market keys — limit to requested markets only
PLAYER_MARKETS = [
    "player_points_rebounds_assists",
    "player_points",
    "player_assists",
    "player_threes",
]


def _url(path: str) -> str:
    return f"{BASE}{API_PREFIX}{path}"


def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "odds-player-props-client/1.0",
        }
    )
    return s


def _american_to_prob(american: Optional[float]) -> Optional[float]:
    if american is None:
        return None
    try:
        ao = float(american)
    except Exception:
        return None
    if ao > 0:
        return 100.0 / (ao + 100.0)
    return (-ao) / ((-ao) + 100.0)


def fetch_nba_events(session: requests.Session) -> List[Dict[str, Any]]:
    url = _url("/sports/basketball_nba/events")
    params = {"apiKey": API_KEY, "regions": "us", "dateFormat": "iso"}
    resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


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
    resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


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
            bk_key = bookmaker.get("key")
            bk_title = bookmaker.get("title")
            bk_last_update = bookmaker.get("last_update")

            for market in bookmaker.get("markets", []) or []:
                mkey = market.get("key")
                m_last_update = market.get("last_update")
                outcomes = market.get("outcomes", []) or []

                implieds: List[float] = []
                for o in outcomes:
                    implieds.append(_american_to_prob(o.get("price")) or 0.0)

                total = sum(implieds) or 0.0
                if total > 0:
                    no_vig_probs = [p / total for p in implieds]
                else:
                    n = max(1, len(outcomes))
                    no_vig_probs = [1.0 / n] * n

                for idx, o in enumerate(outcomes):
                    price = o.get("price")
                    implied = implieds[idx] if idx < len(implieds) else None
                    no_vig = no_vig_probs[idx] if idx < len(no_vig_probs) else None

                    rows.append(
                        {
                            "event_id": event_id,
                            "home_team": home,
                            "away_team": away,
                            "commence_time": commence,
                            "bookmaker": bk_key,
                            "bookmaker_title": bk_title,
                            "bookmaker_last_update": bk_last_update,
                            "market_last_update": m_last_update,
                            "market": mkey,
                            "outcome": o.get("name"),
                            "description": o.get("description", ""),
                            "point": o.get("point") if "point" in o else None,
                            "price_american": price,
                            "implied_prob": round(implied, 6)
                            if implied is not None
                            else None,
                            "no_vig_prob": round(no_vig, 6) if no_vig is not None else None,
                        }
                    )

    return rows


def filter_fanduel_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only FanDuel rows where the outcome is Over/Under and market is one we care about."""
    wanted_markets = set(PLAYER_MARKETS)
    out: List[Dict[str, Any]] = []
    for r in rows:
        bk = (r.get("bookmaker") or "").lower()
        market = r.get("market") or ""
        outcome = (r.get("outcome") or "").strip().lower()

        if bk != "fanduel":
            continue
        if market not in wanted_markets:
            continue
        if outcome not in ("over", "under"):
            continue

        out.append(r)
    return out


def find_latest_flattened_json() -> Optional[str]:
    pattern = os.path.join(OUTPUT_DIR, "player_props_basketball_nba_*.json")
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        open(path, "w", encoding="utf-8").close()
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    session = create_session()
    print("Fetching NBA events...")

    try:
        events = fetch_nba_events(session)
    except requests.HTTPError as e:
        print("Failed to fetch events:", e)
        return 1
    except requests.RequestException as e:
        print("Network/API error while fetching events:", e)
        return 1

    today = datetime.utcnow().strftime("%Y%m%d")
    raw_out: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []

    for ev in events:
        eid = ev.get("id")
        if not eid:
            continue

        try:
            resp = fetch_event_player_props(session, eid, PLAYER_MARKETS)
            raw_out.append(resp)

            rows = flatten_events_to_rows([resp])
            all_rows.extend(rows)

            print(f"Processed event {eid}: +{len(rows)} rows")

        except requests.HTTPError as he:
            status = getattr(he.response, "status_code", None)
            print(f"Warning: event {eid} returned HTTP {status}")
        except requests.RequestException as e:
            print(f"Network/API error for event {eid}: {e}")
        except Exception as e:
            print(f"Error fetching event {eid}: {e}")

    fanduel_rows = filter_fanduel_rows(all_rows)

    # FanDuel-only outputs (dated)
    fanduel_json = os.path.join(OUTPUT_DIR, f"fanduel_player_props_basketball_nba_{today}.json")
    fanduel_csv = os.path.join(OUTPUT_DIR, f"fanduel_player_props_basketball_nba_{today}.csv")
    write_json(fanduel_json, fanduel_rows)
    write_csv(fanduel_csv, fanduel_rows)
    print(f"Wrote {fanduel_json} ({len(fanduel_rows)} rows)")
    print(f"Wrote {fanduel_csv} ({len(fanduel_rows)} rows)")

    # Stable filename for GitHub Pages viewer
    cleaned_csv = os.path.join(OUTPUT_DIR, "cleaned_props.csv")
    write_csv(cleaned_csv, fanduel_rows)
    print(f"Wrote {cleaned_csv} ({len(fanduel_rows)} rows)")

    # Flattened JSON for reuse (all rows across all books/markets)
    flat_json = os.path.join(OUTPUT_DIR, f"player_props_basketball_nba_{today}.json")
    write_json(flat_json, all_rows)
    print("Wrote", flat_json)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
