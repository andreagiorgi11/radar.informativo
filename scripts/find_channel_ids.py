import re
import requests


def get_youtube_channel_id(url):
    direct = re.search(r"/channel/(UC[\w-]+)", url)
    if direct:
        return direct.group(1)

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        text = r.text
    except Exception:
        return None

    match = re.search(r'"channelId":"(UC[^"]+)"', text)
    if match:
        return match.group(1)

    match = re.search(r'"browse_id":"(UC[^"]+)"', text)
    if match:
        return match.group(1)

    return None


canali = [
    # --- INVESTIMENTI ---
    ("investire.biz (principale)", "http://www.youtube.com/user/investirebiz"),
    ("investire.biz (analisi)", "http://www.youtube.com/channel/UCtjjRkGJXf8bENTx0-51uOw"),
    ("Vito Lops", "http://www.youtube.com/user/vitoclaps"),
    ("forecaster.biz", "http://www.youtube.com/channel/UCSyg3cbt3l7FAkStQoyyTaw"),
    ("investing.com Italia", "http://www.youtube.com/channel/UCl3XK7i6eQNkG5GXnGlK4BQ"),
    ("investing.com internazionale", "http://www.youtube.com/user/investingcom"),
    ("Marco Casario", "http://www.youtube.com/user/marcocasario"),
    ("Swiss Capital TV", "http://www.youtube.com/channel/UCjrtlxg9ojCZiZUuXn8oIQg"),
    ("Davide D'Isidoro", "http://www.youtube.com/user/disidorodavide"),
    ("Luca Lorenzoni", "http://www.youtube.com/user/YourBoxe"),
    ("Antonio Cioli Puviani", "http://www.youtube.com/user/ciolipuv"),
    ("Francesco Carrino", "http://www.youtube.com/channel/UCWCWphgRsyBosRj2vTeg2yg"),
    ("Ingegneri in Borsa", "http://www.youtube.com/channel/UC0zbDQUYqdac-UPHPTTIjwQ"),
    ("Société Générale (trading)", "http://www.youtube.com/channel/UCGJsCxEoOJEC6iNhizbpsZw"),
    ("Société Générale (corporate)", "http://www.youtube.com/user/societegenerale"),
    # --- CRYPTO (filtro aggressivo) ---
    ("Marco Costanza", "http://www.youtube.com/channel/UCuTGQbyF-NfmNOopYxz0P7g"),
    ("Aftersyde", "http://www.youtube.com/channel/UCyeE5RmrX5foJY_wGX-YPqA"),
    ("Decrypto", "http://www.youtube.com/channel/UCCHX6w7OoXQk5y9zByUsx1g"),
    ("Tiziano Tridico", "http://www.youtube.com/user/TheDrSwa"),
    ("Leonardo Vecchio", "http://www.youtube.com/channel/UC0lLgJ7OajNAKySIukyJFEQ"),
    ("Sicurezza Bitcoin", "http://www.youtube.com/channel/UC8ggTpKXbpfRmfpOrg_hGwQ"),
    ("The Crypto Gateway", "http://www.youtube.com/channel/UC9X2f4pVXSNzsJ2c6ZQVqBQ"),
    ("Fratelli di Crypto", "http://www.youtube.com/channel/UCWrcYj2C3gTFrzYOzQMJRaw"),
    ("Mauro Caimi", "http://www.youtube.com/channel/UCkkP8HpQG0LuMzGUMJzLEMw"),
]


def main():
    for nome, url in canali:
        channel_id = get_youtube_channel_id(url)
        if channel_id:
            rss = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            print(f"[OK] {nome}: {rss}")
        else:
            print(f"[ERR] {nome}: channel_id non trovato - verifica URL manualmente")


if __name__ == "__main__":
    main()
