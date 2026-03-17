import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

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


def parse_feed_with_requests_substack_alt(url: str):
    parsed = urlparse(url)
    host = parsed.netloc.replace("www.", "")
    if "substack.com" not in host:
        return None

    sub = host.split(".")[0].strip()
    if not sub:
        return None

    alt_url = f"https://{sub}.substack.com/feed"
    headers = {
        "User-Agent": "Mozilla/5.0 (RadarInformativo/1.0)",
        "Cookie": "",
    }
    resp = requests.get(alt_url, headers=headers, timeout=25, allow_redirects=True)
    feed = feedparser.parse(resp.content)
    feed["status"] = resp.status_code
    feed["alt_url"] = alt_url
    return feed


def validate_source(source: dict, now: datetime) -> dict:
    try:
        feed = parse_feed_with_requests(source["rss"])
        status = feed.get("status", "N/A")
        if status == 403 and source.get("type") == "substack":
            alt_feed = parse_feed_with_requests_substack_alt(source["rss"])
            if alt_feed is not None:
                feed = alt_feed
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
        if status == 403 and source.get("type") == "substack":
            result["message"] = (
                f"[ERROR]   {source['name']} - status 403 - feed probabilmente riservato ai subscriber paganti"
            )
        else:
            result["message"] = f"[ERROR]   {source['name']} - status {status} - URL da correggere"
        return result

    entries = getattr(feed, "entries", [])
    if not entries:
        result["severity"] = "warn"
        result["message"] = f"[WARN]    {source['name']} - feed senza entry"
        return result

    latest = parse_published(entries[0])
    if not latest:
        result["severity"] = "warn"
        result["message"] = f"[WARN]    {source['name']} - data ultimo post non disponibile"
        return result

    days_ago = (now - latest).days
    date_str = latest.astimezone(timezone.utc).strftime("%d/%m/%Y")
    result["last_post_iso"] = latest.isoformat()
    result["days_ago"] = days_ago

    if days_ago > WARN_DAYS:
        result["severity"] = "warn"
        result["message"] = (
            f"[WARN]    {source['name']} - ultimo post: {date_str} ({days_ago} giorni fa) - fonte silenziosa"
        )
    elif days_ago > 0:
        result["message"] = f"[OK]      {source['name']} - ultimo post: {date_str} ({days_ago} giorni fa)"
    else:
        result["message"] = f"[OK]      {source['name']} - ultimo post: {date_str}"

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
