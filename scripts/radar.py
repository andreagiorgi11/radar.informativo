import feedparser
import yaml
import os
import json
import hashlib
import sys
import subprocess
import re
import requests
import time
import html
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


load_dotenv()
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


CONFIG = {
    "finestra_ore": 24,
    "min_durata_secondi": 240,
    "min_wpm": 80,
    "min_parole_video_lungo": 500,
    "min_citazioni_fonte_candidata": 3,
    "seen_ttl_ore": 72,
    # Limita il costo transcript su GitHub Actions per run più stabile.
    "max_transcript_youtube_per_run": 12,
    "yt_dlp_timeout_sec": 90,
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_feed_validation(output_dir: Path) -> list[str]:
    path = output_dir / "feed_health_latest.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        items = payload.get("items", [])
        return [it.get("message", "").strip() for it in items if it.get("message")]
    except Exception:
        return []


def normalize_text(text: str) -> str:
    if not text:
        return ""
    clean = BeautifulSoup(text, "html.parser").get_text(" ")
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def parse_published(entry) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        if hasattr(entry, key) and getattr(entry, key):
            return datetime(*getattr(entry, key)[:6], tzinfo=timezone.utc)
    return None


def parse_duration_seconds(entry) -> int | None:
    candidates = []
    yt_duration = getattr(entry, "yt_duration", None)
    if yt_duration:
        candidates.append(str(yt_duration))

    media_content = getattr(entry, "media_content", None)
    if media_content and isinstance(media_content, list):
        for m in media_content:
            if isinstance(m, dict):
                duration = m.get("duration")
                if duration:
                    candidates.append(str(duration))

    summary = normalize_text(getattr(entry, "summary", ""))
    if summary:
        match_clock = re.search(r"\b(\d{1,2}):(\d{2})(?::(\d{2}))?\b", summary)
        if match_clock:
            if match_clock.group(3):
                h = int(match_clock.group(1))
                m = int(match_clock.group(2))
                s = int(match_clock.group(3))
                return h * 3600 + m * 60 + s
            m = int(match_clock.group(1))
            s = int(match_clock.group(2))
            return m * 60 + s

    for raw in candidates:
        if raw.isdigit():
            return int(raw)
        iso = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", raw)
        if iso:
            h = int(iso.group(1) or 0)
            m = int(iso.group(2) or 0)
            s = int(iso.group(3) or 0)
            return h * 3600 + m * 60 + s
    return None


def infer_flusso_from_category(category: str) -> str:
    ai_categories = {"ai_modelli", "ai_usecase", "ai_tool", "ai"}
    return "ai" if category in ai_categories else "investimenti"


def carica_fonti(feeds_path: Path) -> tuple[list, list]:
    data = yaml.safe_load(feeds_path.read_text(encoding="utf-8")) or {}
    fonti_attive = []
    fonti_validazione = []

    for flusso in ("investimenti", "ai"):
        for fonte in data.get(flusso, []) or []:
            item = dict(fonte)
            item["flusso"] = flusso
            item["in_validazione"] = False
            if "priorità" not in item:
                item["priorità"] = item.get("priority", "media")
            fonti_attive.append(item)

    for fonte in data.get("validazione", []) or []:
        item = dict(fonte)
        item["flusso"] = infer_flusso_from_category(item.get("category", ""))
        item["in_validazione"] = True
        if "priorità" not in item:
            item["priorità"] = item.get("priority", "media")
        fonti_validazione.append(item)

    return fonti_attive, fonti_validazione


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


def fetch_rss(fonte: dict) -> list[dict]:
    ritardi = [3, 10, 30]
    last_error = None
    feed = None

    for i, ritardo in enumerate(ritardi):
        try:
            feed = parse_feed_with_requests(fonte["rss"])
            status = getattr(feed, "status", 200)
            if status == 403 and fonte.get("type") == "substack":
                alt_feed = parse_feed_with_requests_substack_alt(fonte["rss"])
                if alt_feed is not None and getattr(alt_feed, "status", 200) == 200:
                    feed = alt_feed
                    status = 200
            if status != 200:
                raise RuntimeError(f"HTTP status {status}")
            break
        except Exception as exc:
            last_error = exc
            if i < len(ritardi) - 1:
                time.sleep(ritardo)

    if feed is None:
        print(f"[ERRORE FEED] {fonte['name']} | {fonte['rss']} | {last_error}")
        return []

    limite = now_utc() - timedelta(hours=CONFIG["finestra_ore"])
    items = []

    for entry in getattr(feed, "entries", []):
        pub = parse_published(entry)
        if not pub or pub < limite:
            continue

        item = {
            "titolo": getattr(entry, "title", "(senza titolo)"),
            "link": getattr(entry, "link", ""),
            "data_pubblicazione": pub.isoformat(),
            "descrizione": normalize_text(getattr(entry, "summary", "")),
            "fonte_name": fonte["name"],
            "categoria": fonte.get("category", ""),
            "priorità": fonte.get("priorità", "media"),
            "type": fonte.get("type", ""),
            "flusso": fonte.get("flusso", "investimenti"),
            "in_validazione": fonte.get("in_validazione", False),
        }
        if item["type"] == "youtube":
            item["durata_secondi"] = parse_duration_seconds(entry)
        else:
            item["durata_secondi"] = None
        items.append(item)

    return items


def load_seen(seen_path: Path) -> dict:
    if seen_path.exists():
        try:
            seen = json.loads(seen_path.read_text(encoding="utf-8"))
        except Exception:
            seen = {}
    else:
        seen = {}

    now = now_utc()
    ttl = timedelta(hours=CONFIG["seen_ttl_ore"])
    return {
        h: ts
        for h, ts in seen.items()
        if datetime.fromisoformat(ts) > now - ttl
    }


def save_seen(seen_path: Path, seen: dict) -> None:
    seen_path.write_text(json.dumps(seen, indent=2), encoding="utf-8")


def is_duplicate(url: str, seen: dict) -> bool:
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return url_hash in seen


def mark_seen(url: str, seen: dict) -> None:
    url_hash = hashlib.md5(url.encode()).hexdigest()
    seen[url_hash] = now_utc().isoformat()


def parse_vtt(vtt_path: Path) -> str:
    lines = vtt_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    text_lines = []

    for line in lines:
        raw = line.strip()
        if not raw:
            continue
        if raw.startswith("WEBVTT"):
            continue
        if "-->" in raw:
            continue
        if raw.isdigit():
            continue
        if raw.upper().startswith("NOTE"):
            continue
        if raw.startswith("Kind:") or raw.startswith("Language:"):
            continue
        clean = re.sub(r"<[^>]+>", "", raw)
        clean = html.unescape(clean).strip()
        if clean:
            text_lines.append(clean)

    deduped = []
    prev = None
    for ln in text_lines:
        if ln != prev:
            deduped.append(ln)
        prev = ln
    return " ".join(deduped).strip()


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text or ""))


def get_ytdlp_extra_args() -> list[str]:
    args: list[str] = []
    if shutil.which("node"):
        args.extend(["--js-runtimes", "node"])
        args.extend(["--remote-components", "ejs:github"])

    cookies_path = os.environ.get("YTDLP_COOKIES_PATH")
    if cookies_path and Path(cookies_path).exists():
        args.extend(["--cookies", cookies_path])
    return args


def get_video_info(video_url: str) -> dict:
    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--skip-download",
        "--no-warnings",
    ]
    cmd.extend(get_ytdlp_extra_args())
    cmd.append(video_url)
    ritardi = [3, 8]
    for i, ritardo in enumerate(ritardi):
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=45,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                return json.loads(proc.stdout)
        except Exception:
            pass
        if i < len(ritardi) - 1:
            time.sleep(ritardo)
    return {}


def build_sub_lang_order(info: dict) -> list[str]:
    order = []

    def add_lang(lang: str | None):
        if not lang:
            return
        l = str(lang).strip()
        if not l:
            return
        base = l.split("-")[0].lower()
        if base not in order:
            order.append(base)

    metadata_lang = info.get("language")
    add_lang(metadata_lang)

    subtitles = info.get("subtitles") or {}
    automatic = info.get("automatic_captions") or {}
    declared = list(subtitles.keys()) + list(automatic.keys())
    for lang in declared[:4]:
        add_lang(lang)

    add_lang("it")
    add_lang("en")

    if declared:
        best = declared[0].split("-")[0].lower()
        if best not in order:
            order.append(best)

    return order[:5]


def run_yt_dlp(video_url: str, tempdir: Path, auto: bool, sub_lang: str) -> tuple[Path | None, bool, bool]:
    cmd = [
        "yt-dlp",
        "--skip-download",
        "--sub-lang",
        sub_lang,
        "--sub-format",
        "vtt",
        "--no-warnings",
        "--output",
        str(tempdir / "%(id)s.%(ext)s"),
    ]
    cmd.extend(get_ytdlp_extra_args())
    if auto:
        cmd.append("--write-auto-sub")
    else:
        cmd.append("--write-sub")
    cmd.append(video_url)

    ritardi = [3]
    last_error = None
    for i, ritardo in enumerate(ritardi):
        try:
            proc = subprocess.run(
                cmd,
                cwd=tempdir,
                capture_output=True,
                text=True,
                check=False,
                timeout=CONFIG["yt_dlp_timeout_sec"],
            )
            if proc.returncode == 0:
                vtts = sorted(tempdir.glob("*.vtt"), key=lambda p: p.stat().st_mtime, reverse=True)
                if vtts:
                    return vtts[0], True, False
            err_text = (proc.stderr or "") + "\n" + (proc.stdout or "")
            if "Sign in to confirm you" in err_text:
                return None, False, True
            last_error = RuntimeError(proc.stderr.strip() or f"exit {proc.returncode}")
        except Exception as exc:
            last_error = exc
        if i < len(ritardi) - 1:
            time.sleep(ritardo)

    if last_error:
        print(f"[WARN] yt-dlp fallito su {video_url}: {last_error}")
    return None, False, False


def get_transcript(video_url: str, durata_secondi: int | None = None) -> dict:
    result = {
        "text": None,
        "transcript_text": None,
        "quality": "assente",
        "transcript_quality": "assente",
        "word_count": 0,
        "flag_sospetto": False,
        "motivo_flag": None,
    }

    # Shorts spesso triggerano anti-bot su yt-dlp e non servono al radar long-form.
    if "/shorts/" in (video_url or ""):
        return result

    info = get_video_info(video_url)
    lang_order = build_sub_lang_order(info)

    with tempfile.TemporaryDirectory(prefix="radar_sub_") as tmp:
        tempdir = Path(tmp)
        vtt_path = None
        quality = "assente"

        for sub_lang in lang_order:
            vtt_path, ok, blocked = run_yt_dlp(video_url, tempdir, auto=False, sub_lang=sub_lang)
            quality = "manuale"
            if blocked:
                return result
            if ok:
                break

            vtt_path, ok, blocked = run_yt_dlp(video_url, tempdir, auto=True, sub_lang=sub_lang)
            quality = "automatico"
            if blocked:
                return result
            if ok:
                break

        if vtt_path is None:
            return result

        text = parse_vtt(vtt_path)
        word_count = count_words(text)

        flag_sospetto = False
        motivo_flag = None
        if durata_secondi and durata_secondi > 0:
            durata_min = durata_secondi / 60.0
            wpm = word_count / durata_min if durata_min else 0
            if wpm < CONFIG["min_wpm"]:
                flag_sospetto = True
                motivo_flag = f"{int(round(wpm))} wpm (soglia: {CONFIG['min_wpm']})"
            elif durata_secondi > 600 and word_count < CONFIG["min_parole_video_lungo"]:
                flag_sospetto = True
                minuti = int(round(durata_min))
                motivo_flag = f"video {minuti} min, solo {word_count} parole"

        result.update(
            {
                "text": text,
                "transcript_text": text,
                "quality": quality,
                "transcript_quality": quality,
                "word_count": word_count,
                "flag_sospetto": flag_sospetto,
                "motivo_flag": motivo_flag,
            }
        )
        return result


def applica_filtri(item: dict) -> tuple[bool, str]:
    if item["type"] == "youtube":
        if "/shorts/" in (item.get("link", "") or ""):
            return False, "YouTube Short — nessun transcript disponibile"
        if item.get("durata_secondi") and item["durata_secondi"] < CONFIG["min_durata_secondi"]:
            return False, f"video troppo corto ({item['durata_secondi']}s)"

    pattern_promo = [
        "sponsored",
        "#ad",
        "promo code",
        "discount",
        "coupon",
        "use code",
        "affiliate",
        "giveaway",
        "win a",
        "free gift",
    ]
    titolo_lower = item["titolo"].lower()
    for pattern in pattern_promo:
        if pattern in titolo_lower:
            return False, f"pattern promozionale: '{pattern}'"

    testo = (item["titolo"] + " " + item.get("descrizione", "")).lower()
    keyword_rilevanti = [
        "macro",
        "market",
        "rate",
        "fed",
        "inflation",
        "deflation",
        "crypto",
        "bitcoin",
        "ethereum",
        "defi",
        "onchain",
        "equity",
        "stock",
        "earnings",
        "valuation",
        "sector",
        "chip",
        "semiconductor",
        "nvidia",
        "tsmc",
        "supply chain",
        "ai",
        "model",
        "agent",
        "llm",
        "gpt",
        "claude",
        "gemini",
        "liquidity",
        "yield",
        "bond",
        "treasury",
        "recession",
        "investing",
        "portfolio",
        "hedge",
        "fund",
        "vc",
    ]
    if not any(kw in testo for kw in keyword_rilevanti):
        return False, "fuori tema (nessuna keyword rilevante)"

    return True, ""


def calcola_score(item: dict) -> float:
    score = 5.0
    if item.get("priorità") == "alta":
        score += 2.0

    if item.get("transcript_quality") == "manuale":
        score += 1.0
    elif item.get("transcript_quality") == "assente":
        score -= 1.0

    if item.get("word_count", 0) > 2000:
        score += 1.0

    if item["type"] == "substack":
        score += 0.5

    if item.get("flag_sospetto"):
        score -= 2.0

    if item.get("in_validazione"):
        score -= 1.0

    return round(min(max(score, 0.0), 10.0), 1)


def scopri_fonti_candidate(items: list, fonti_attive_domini: set) -> list:
    cited = Counter()
    for item in items:
        if item["type"] != "substack":
            continue
        testo = item.get("transcript_text") or item.get("descrizione", "")
        urls = re.findall(r"https?://[^\s\)\"']+", testo)
        for url in urls:
            domain = urlparse(url).netloc.replace("www.", "")
            if domain and domain not in fonti_attive_domini:
                cited[domain] += 1

    return [
        {"domain": domain, "citazioni": count}
        for domain, count in cited.most_common(10)
        if count >= CONFIG["min_citazioni_fonte_candidata"]
    ]


def format_transcript_status(item: dict) -> str:
    quality = item.get("transcript_quality")
    if quality == "manuale":
        return "✅ disponibile (manuale)"
    if quality == "automatico":
        return "⚡ disponibile (automatico)"
    if item["type"] == "substack":
        return "✅ disponibile (manuale)"
    return "❌ assente"


def format_flag(item: dict) -> str:
    if item.get("flag_sospetto"):
        motivo = item.get("motivo_flag") or "transcript sospetto"
        return f"⚠️ transcript sospetto — {motivo}"
    return "✅ nessuno"


def extract_snippet(item: dict, max_len: int = 600) -> str:
    raw_text = item.get("transcript_text") or item.get("text") or item.get("descrizione", "")
    righe = raw_text.split("\n")
    righe_pulite = [r for r in righe if not r.startswith(("Kind:", "Language:"))]
    text = "\n".join(righe_pulite).strip()
    text = normalize_text(text)
    if len(text) <= max_len:
        return text or "(nessun estratto disponibile)"
    return text[:max_len].rstrip() + "..."


def render_items(items: list, validazione: bool = False) -> str:
    if not items:
        return "_Nessun contenuto rilevante in questa run._\n"

    lines = []
    for item in items:
        title = item["titolo"]
        source = item["fonte_name"]
        valid_label = " **DA VALIDARE**" if validazione else ""
        lines.append(f"### [{item['score']}] {title} — {source}{valid_label}")
        lines.append(f"- **Tipo**: {'YouTube' if item['type'] == 'youtube' else 'Substack'}")
        lines.append(f"- **Categoria**: {item.get('categoria', '-')}")
        lines.append(f"- **Transcript**: {format_transcript_status(item)}")
        lines.append(f"- **Flag**: {format_flag(item)}")
        lines.append(f"- **Link**: {item['link']}")
        lines.append(f"- **Estratto**: {extract_snippet(item)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def genera_markdown(
    items_investimenti: list,
    items_ai: list,
    items_validazione: list,
    fonti_candidate: list,
    run_label: str,
    stats: dict,
    feed_validation_lines: list[str],
) -> str:
    now_it = datetime.now(ZoneInfo("Europe/Rome"))
    data_italiana = now_it.strftime("%d/%m/%Y")

    candidate_rows = []
    if fonti_candidate:
        for c in fonti_candidate:
            candidate_rows.append(f"| {c['domain']} | {c['citazioni']} |")
    else:
        candidate_rows.append("| _Nessuna fonte candidata_ | 0 |")

    md = f"""# Radar Informativo — {data_italiana} | Run {run_label}
*Generato automaticamente | {stats['fonti_monitorate']} fonti monitorate | {stats['item_processati']} item processati | {stats['pubblicati_report']} pubblicati*

---

## 📥 INVESTIMENTI / MACRO / STOCK / CRYPTO / SEMIS

{render_items(items_investimenti)}
---

## 📥 AI / MODELLI / TOOL / USE CASE

{render_items(items_ai)}
---

## 🔍 FONTI IN VALIDAZIONE

{render_items(items_validazione, validazione=True)}
---

## 🌐 FONTI CANDIDATE (scoperte automaticamente)

| Dominio | Volte citato |
|---|---|
{os.linesep.join(candidate_rows)}

*Valuta se aggiungere queste fonti a feeds.yml*

---

## 📊 STATISTICHE RUN

| Metrica | Valore |
|---|---|
| Fonti monitorate | {stats['fonti_monitorate']} |
| Item RSS trovati | {stats['item_rss_trovati']} |
| Scartati (duplicati) | {stats['scartati_duplicati']} |
| Scartati (filtro durata) | {stats['scartati_filtro_durata']} |
| Scartati (fuori tema) | {stats['scartati_fuori_tema']} |
| Scartati (promozionali) | {stats['scartati_promozionali']} |
| Pubblicati nel report | {stats['pubblicati_report']} |
| Transcript disponibili | {stats['transcript_disponibili']} |
| Transcript sospetti | {stats['transcript_sospetti']} |

---

## 🩺 VALIDAZIONE FEED

{os.linesep.join(feed_validation_lines) if feed_validation_lines else "_Nessun log validazione feed disponibile._"}

---

## 🐦 INTEGRAZIONE X — STEP MANUALE

**Completa questo passaggio PRIMA di incollare il file in ChatGPT.**

### 1. Copia questo prompt e incollalo su Grok

```
Nelle ultime 24 ore, cosa hanno scritto di rilevante su macro mercati,
tassi, liquidità, crypto, Bitcoin, semiconduttori e AI i profili che
seguo? Includi solo:
- tweet con dati, numeri o tesi originali
- thread con analisi strutturata
- segnali anticipatori su mercati o tecnologia

Escludi:
- retweet senza commento
- contenuti promozionali o referral
- opinioni senza evidenze
- discussioni personali

Formato output: per ogni contenuto rilevante scrivi —
AUTORE | TEMA | SINTESI IN 2 RIGHE | LINK
```

### 2. Incolla qui la risposta di Grok

[INCOLLA QUI L'OUTPUT DI GROK]

---

### 3. Ora incolla tutto questo file in ChatGPT

---

> ⚠️ **ISTRUZIONI PER CHATGPT — LEGGERE PRIMA DI INIZIARE**
>
> Questo file contiene il materiale grezzo del Radar Informativo giornaliero.
> Prima di iniziare qualsiasi analisi:
>
> 1. Controlla se nella sezione "INTEGRAZIONE X" è presente output di Grok
>    - Se presente: includilo nell'analisi come fonte aggiuntiva sul sentiment e segnali real-time
>    - Se assente: procedi comunque con i contenuti disponibili senza chiedere conferma
>
> 2. Produci il brief finale in italiano con questa struttura:
>    - 🔴 SEGNALE FORTE DEL GIORNO (se presente, altrimenti ometti)
>    - 🏆 TOP 5 INSIGHT DEL GIORNO
>    - 📊 BRIEF MACRO
>    - 📈 BRIEF STOCK
>    - 🔶 BRIEF CRYPTO
>    - 🏭 BRIEF SETTORI / SEMIS
>    - 🤖 BRIEF AI
>    - 🌐 NUOVE FONTI DA VALUTARE
>
> 3. Per ogni insight: cita sempre la fonte e il link originale
> 4. Ignora contenuti con flag ⚠️ transcript sospetto a meno che il titolo
>    non sia già di per sé informativo
> 5. Non chiedere conferma. Inizia direttamente con il brief.
"""
    return md


def build_drive_service(
    credentials_json: str | None,
    oauth_client_id: str | None,
    oauth_client_secret: str | None,
    oauth_refresh_token: str | None,
):
    scopes = ["https://www.googleapis.com/auth/drive"]
    if oauth_client_id and oauth_client_secret and oauth_refresh_token:
        credentials = UserCredentials(
            token=None,
            refresh_token=oauth_refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=oauth_client_id,
            client_secret=oauth_client_secret,
            scopes=scopes,
        )
        return build("drive", "v3", credentials=credentials), "oauth"

    if credentials_json:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(credentials_json),
            scopes=scopes,
        )
        return build("drive", "v3", credentials=credentials), "service_account"

    return None, None


def upload_to_drive(filepath: Path, folder_id: str, service) -> str:

    query = f"name='{filepath.name}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=query, fields="files(id)").execute()

    media = MediaFileUpload(str(filepath), mimetype="text/markdown")

    if existing["files"]:
        file_id = existing["files"][0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
    else:
        metadata = {"name": filepath.name, "parents": [folder_id]}
        result = service.files().create(
            body=metadata,
            media_body=media,
            fields="id",
        ).execute()
        file_id = result["id"]

    return file_id


def main():
    run_label = os.environ.get("RUN_LABEL", "MANUALE")

    repo_root = Path(__file__).parent.parent
    feeds_path = repo_root / "sources" / "feeds.yml"
    output_dir = repo_root / "output"
    seen_path = output_dir / "seen.json"
    output_dir.mkdir(exist_ok=True)

    now_it = datetime.now(ZoneInfo("Europe/Rome"))
    filename = f"{now_it.strftime('%Y-%m-%d_%H00')}.md"
    output_path = output_dir / filename

    seen = load_seen(seen_path)
    save_seen(seen_path, seen)

    fonti_attive, fonti_validazione = carica_fonti(feeds_path)

    stats = {
        "fonti_monitorate": len(fonti_attive) + len(fonti_validazione),
        "item_rss_trovati": 0,
        "item_processati": 0,
        "scartati_duplicati": 0,
        "scartati_filtro_durata": 0,
        "scartati_fuori_tema": 0,
        "scartati_promozionali": 0,
        "pubblicati_report": 0,
        "transcript_disponibili": 0,
        "transcript_sospetti": 0,
    }

    tutti_items = []
    transcript_youtube_count = 0
    for fonte in fonti_attive + fonti_validazione:
        items_raw = fetch_rss(fonte)
        stats["item_rss_trovati"] += len(items_raw)
        for item in items_raw:
            stats["item_processati"] += 1
            if not item.get("link"):
                continue

            if is_duplicate(item["link"], seen):
                stats["scartati_duplicati"] += 1
                continue

            passa, motivo = applica_filtri(item)
            if not passa:
                print(f"[SCARTATO] {item['titolo'][:80]} — {motivo}")
                if "video troppo corto" in motivo:
                    stats["scartati_filtro_durata"] += 1
                elif "pattern promozionale" in motivo:
                    stats["scartati_promozionali"] += 1
                elif "fuori tema" in motivo:
                    stats["scartati_fuori_tema"] += 1
                continue

            if item["type"] == "youtube":
                if transcript_youtube_count < CONFIG["max_transcript_youtube_per_run"]:
                    transcript_data = get_transcript(item["link"], item.get("durata_secondi"))
                    item.update(transcript_data)
                    transcript_youtube_count += 1
                    if item.get("transcript_quality") in {"manuale", "automatico"}:
                        stats["transcript_disponibili"] += 1
                    if item.get("flag_sospetto"):
                        stats["transcript_sospetti"] += 1
                else:
                    item.update(
                        {
                            "text": None,
                            "transcript_text": None,
                            "quality": "assente",
                            "transcript_quality": "assente",
                            "word_count": 0,
                            "flag_sospetto": False,
                            "motivo_flag": None,
                        }
                    )
            else:
                item["transcript_text"] = item.get("descrizione", "")
                item["word_count"] = count_words(item["transcript_text"])

            item["score"] = calcola_score(item)
            tutti_items.append(item)
            mark_seen(item["link"], seen)
            save_seen(seen_path, seen)

    items_inv = sorted(
        [i for i in tutti_items if i["flusso"] == "investimenti" and not i.get("in_validazione")],
        key=lambda x: x["score"],
        reverse=True,
    )
    items_ai = sorted(
        [i for i in tutti_items if i["flusso"] == "ai" and not i.get("in_validazione")],
        key=lambda x: x["score"],
        reverse=True,
    )
    items_val = sorted(
        [i for i in tutti_items if i.get("in_validazione")],
        key=lambda x: x["score"],
        reverse=True,
    )
    stats["pubblicati_report"] = len(items_inv) + len(items_ai) + len(items_val)

    fonti_attive_domini = {
        urlparse(f["rss"]).netloc.replace("www.", "")
        for f in fonti_attive
    }
    fonti_candidate = scopri_fonti_candidate(tutti_items, fonti_attive_domini)

    feed_validation_lines = load_feed_validation(output_dir)
    md_content = genera_markdown(
        items_inv,
        items_ai,
        items_val,
        fonti_candidate,
        run_label,
        stats,
        feed_validation_lines,
    )
    output_path.write_text(md_content, encoding="utf-8")
    print(f"[OK] File generato: {filename}")

    credentials_json = os.environ.get("GDRIVE_CREDENTIALS")
    oauth_client_id = os.environ.get("GDRIVE_OAUTH_CLIENT_ID")
    oauth_client_secret = os.environ.get("GDRIVE_OAUTH_CLIENT_SECRET")
    oauth_refresh_token = os.environ.get("GDRIVE_OAUTH_REFRESH_TOKEN")
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    service, auth_mode = build_drive_service(
        credentials_json,
        oauth_client_id,
        oauth_client_secret,
        oauth_refresh_token,
    )
    if service and folder_id:
        try:
            file_id = upload_to_drive(output_path, folder_id, service)
            print(f"[OK] Caricato su Drive ({auth_mode}): {file_id}")
        except Exception as exc:
            print(f"[WARN] Upload Drive fallito, run completata comunque: {exc}")
    else:
        print("[WARN] Credenziali Drive non trovate, skip upload")


if __name__ == "__main__":
    main()
