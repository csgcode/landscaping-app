import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

ENV = os.getenv("ENVIRONMENT", "dev")
WEATHER_TRACKING_TABLE = os.getenv("WEATHER_TRACKING_TABLE_NAME")
WEATHER_CACHE_TABLE = os.getenv("WEATHER_CACHE_TABLE_NAME")
WEATHER_API_KEY_SECRET_ARN = os.getenv("WEATHER_API_KEY_SECRET_ARN", "")

MAX_LOCATIONS_PER_CYCLE = int(os.getenv("MAX_LOCATIONS_PER_CYCLE", "200"))
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "7"))

dynamo = boto3.resource("dynamodb")
tracking_tbl = dynamo.Table(WEATHER_TRACKING_TABLE)
cache_tbl = dynamo.Table(WEATHER_CACHE_TABLE)
secrets = boto3.client("secretsmanager")

logger = logging.getLogger("weather-daily-refresh")
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def start_of_day_utc(dt: datetime) -> datetime:
    d = dt.astimezone(timezone.utc)
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

def days_between(a: datetime, b: datetime) -> int:
    """Whole days from a -> b, floor at 0."""
    delta = (b - a).total_seconds() / 86400.0
    return int(delta) if delta > 0 else 0

def get_api_key() -> Optional[str]:
    if not WEATHER_API_KEY_SECRET_ARN:
        return None
    try:
        out = secrets.get_secret_value(SecretId=WEATHER_API_KEY_SECRET_ARN)
        return out.get("SecretString")
    except ClientError as e:
        logger.warning(f"weather api key fetch failed: {e}")
        return None

def select_due_tracking(now_utc: datetime) -> List[dict]:
    """
    Query GSI by_status_nextcheck for items due within the next LOOKAHEAD_DAYS window.
    next_check_at <= now + LOOKAHEAD_DAYS
    """
    window_end = (now_utc + timedelta(days=LOOKAHEAD_DAYS)).isoformat()
    items: List[dict] = []
    last = None
    while True:
        params = {
            "IndexName": "by_status_nextcheck",
            "KeyConditionExpression": "#s = :active AND #n <= :until",
            "ExpressionAttributeNames": {"#s": "status", "#n": "next_check_at"},
            "ExpressionAttributeValues": {":active": "active", ":until": window_end},
            "Limit": MAX_LOCATIONS_PER_CYCLE,
        }
        if last:
            params["ExclusiveStartKey"] = last
        resp = tracking_tbl.query(**params)
        items.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last or len(items) >= MAX_LOCATIONS_PER_CYCLE:
            break
    return items[:MAX_LOCATIONS_PER_CYCLE]

def group_by_location_with_horizon(now_utc: datetime, rows: List[dict]) -> Dict[str, int]:
    """
    For each postcode, compute how many days to fetch:
      horizon_days = min(LOOKAHEAD_DAYS, max(days_left across rows for that location, at least 1))
    """
    per_loc_max_days: Dict[str, int] = {}
    for r in rows:
        loc = r.get("location")
        sched_at = r.get("scheduled_at")
        if not loc or not sched_at:
            continue
        try:
            sched_dt = datetime.fromisoformat(sched_at.replace("Z", "+00:00"))
        except Exception:
            continue
        days_left = days_between(start_of_day_utc(now_utc), start_of_day_utc(sched_dt))
        # clamp to [1, LOOKAHEAD_DAYS]
        need = max(1, min(LOOKAHEAD_DAYS, days_left))
        per_loc_max_days[loc] = max(per_loc_max_days.get(loc, 1), need)
    return per_loc_max_days

def fetch_forecast_days(postcode: str, days: int, api_key: Optional[str]) -> List[Dict]:
    """
    Return a list of daily risk dicts for [today .. today+days-1].
    In real impl: call provider, map precip/wind/etc -> risk.
    """
    today = start_of_day_utc(utcnow())
    out: List[Dict] = []
    for i in range(days):
        day = today + timedelta(days=i)
        if not api_key:
            risk, reason = "unknown", "no_api_key"
        else:
            # TODO integrate real provider thresholds
            risk, reason = "low", "clear_forecast"
        out.append({
            "date": day.strftime("%Y-%m-%d"),
            "risk": risk,
            "reason": reason,
            "updated_at": iso(utcnow()),
        })
    return out

def cache_key_day(postcode: str, date_yyyy_mm_dd: str) -> str:
    # Example: "SW1A 2AA#2025-11-05"
    return f"{postcode}#{date_yyyy_mm_dd}"

def write_daily_cache(postcode: str, days_data: List[Dict]):
    with cache_tbl.batch_writer(overwrite_by_pkeys=["cache_key"]) as batch:
        for d in days_data:
            item = {
                "cache_key": cache_key_day(postcode, d["date"]),
                "postcode": postcode,
                "risk": d["risk"],
                "reason": d["reason"],
                "updated_at": d["updated_at"],
                # TTL ~ 10 days keeps overlap for consumers
                "ttl": int((utcnow() + timedelta(days=10)).timestamp()),
            }
            batch.put_item(Item=item)

def bump_next_check_for_rows(rows: List[dict], now_utc: datetime):
    """
    For each row just serviced (passed in), move next_check_at forward.
    Simple cadence:
      days_left > 3  -> +24h
      1 < days_left ≤ 3 -> +6h
      ≤ 1 -> +3h
    """
    for r in rows:
        tid = r["tracking_id"]
        loc = r.get("location")
        sched_at = r.get("scheduled_at")
        if not sched_at:
            continue
        try:
            sched_dt = datetime.fromisoformat(sched_at.replace("Z", "+00:00"))
        except Exception:
            continue
        days_left = days_between(start_of_day_utc(now_utc), start_of_day_utc(sched_dt))
        if days_left > 3:
            delta = timedelta(hours=24)
        elif days_left > 1:
            delta = timedelta(hours=6)
        else:
            delta = timedelta(hours=3)
        new_next = iso(now_utc + delta)
        try:
            tracking_tbl.update_item(
                Key={"tracking_id": tid},
                UpdateExpression="SET next_check_at = :n, updated_at = :u",
                ExpressionAttributeValues={":n": new_next, ":u": iso(now_utc)},
                ConditionExpression="attribute_exists(tracking_id)"
            )
        except ClientError as e:
            logger.warning(f"bump failed for {tid} ({loc}): {e}")

def handler(event, context):
    run_id = utcnow().strftime("%Y%m%d%H%M%S")
    correlation_id = f"weather-daily-{run_id}"
    now = utcnow()

    due_rows = select_due_tracking(now)
    if not due_rows:
        logger.info(f"[{correlation_id}] no due rows")
        return {"status": "ok", "refreshed_locations": 0, "correlation_id": correlation_id}

    per_loc_days = group_by_location_with_horizon(now, due_rows)
    if not per_loc_days:
        logger.info(f"[{correlation_id}] no locations extracted from due rows")
        return {"status": "ok", "refreshed_locations": 0, "correlation_id": correlation_id}

    api_key = get_api_key()
    refreshed = 0

    rows_by_loc: Dict[str, List[dict]] = {}
    for r in due_rows:
        loc = r.get("location")
        if not loc:
            continue
        rows_by_loc.setdefault(loc, []).append(r)

    for postcode, days in per_loc_days.items():
        try:
            days_data = fetch_forecast_days(postcode, days, api_key)
            write_daily_cache(postcode, days_data)
            refreshed += 1
            bump_next_check_for_rows(rows_by_loc.get(postcode, []), now)
        except Exception as e:
            logger.error(f"[{correlation_id}] refresh failed for {postcode}: {e}")

    logger.info(f"[{correlation_id}] refreshed {refreshed} location(s)")
    return {"status": "ok", "refreshed_locations": refreshed, "correlation_id": correlation_id}
