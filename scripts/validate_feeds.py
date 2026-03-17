import json
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
import yaml


WARN_DAYS = 21


def parse_published(entry) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        value = getattr(entry, key, None)
        if value:
            return datetime(*value[:6], tzinfo=timezone.utc)
    return None


def load_sources(feeds_path: Path) -> list[dict]:
    data = yaml.safe_load(feeds_path.read_text(encoding="utf-8")) or {}
    sources = []
    for section in ("investimenti", "ai", "validazione"):
        for src in data.get(section, []) or []:
            item = dict(src)
            item["section"] = section
            sources.append(item)
    return sources


def parse_feed_with_requests(url: str):
    headers = {"User-Agent": "Mozilla/5.0 (RadarInformativo/1.0)"}
    resp = requests.get(url, headers=headers, timeout=25, allow_redirects=True)
    feed = feedparser.parse(resp.content)
    feed["status"] = resp.status_code
    return feed


def validate_source(source: dict, now: datetime) -> dict:
    try:
        feed = parse_feed_with_requests(source["rss"])
    except Exception as exc:
        feed = {"status": f"ERR:{exc.__class__.__name__}", "entries": []}
    status = feed.get("status", "N/A")

    result = {
        "name": source["name"],
        "rss": source["rss"],
        "status": status,
        "severity": "ok",
        "message": "",
        "last_post_iso": None,
        "days_ago": None,
    }

    if status != 200:
        result["severity"] = "error"
        result["message"] = f"[ERROR]   {source['name']} — status {status} — URL da correggere"
        return result

    entries = getattr(feed, "entries", [])
    if not entries:
        result["severity"] = "warn"
        result["message"] = f"[WARN]    {source['name']} — feed senza entry"
        return result

    latest = parse_published(entries[0])
    if not latest:
        result["severity"] = "warn"
        result["message"] = f"[WARN]    {source['name']} — data ultimo post non disponibile"
        return result

    days_ago = (now - latest).days
    date_str = latest.astimezone(timezone.utc).strftime("%d/%m/%Y")
    result["last_post_iso"] = latest.isoformat()
    result["days_ago"] = days_ago

    if days_ago > WARN_DAYS:
        result["severity"] = "warn"
        result["message"] = (
            f"[WARN]    {source['name']} — ultimo post: {date_str} ({days_ago} giorni fa) — fonte silenziosa"
        )
    elif days_ago > 0:
        result["message"] = f"[OK]      {source['name']} — ultimo post: {date_str} ({days_ago} giorni fa)"
    else:
        result["message"] = f"[OK]      {source['name']} — ultimo post: {date_str}"

    return result


def main():
    repo_root = Path(__file__).parent.parent
    feeds_path = repo_root / "sources" / "feeds.yml"
    output_dir = repo_root / "output"
    output_dir.mkdir(exist_ok=True)
    out_path = output_dir / "feed_health_latest.json"

    now = datetime.now(timezone.utc)
    sources = load_sources(feeds_path)
    results = [validate_source(src, now) for src in sources]

    for item in results:
        print(item["message"])

    payload = {
        "generated_at": now.isoformat(),
        "warn_days": WARN_DAYS,
        "items": results,
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] Feed validation salvata: {out_path.name}")


if __name__ == "__main__":
    main()
