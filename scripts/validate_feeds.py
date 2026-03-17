import json
from datetime import datetime, timezone
from pathlib import Path

import feedparser
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


def validate_source(source: dict, now: datetime) -> dict:
    feed = feedparser.parse(source["rss"])
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
