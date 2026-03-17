import json
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow


SCOPES = ["https://www.googleapis.com/auth/drive"]


def main():
    root = Path(__file__).parent.parent
    client_secret_path = root / "client_secret.json"

    if not client_secret_path.exists():
        raise FileNotFoundError(
            "Manca client_secret.json nella root del progetto. "
            "Scaricalo da Google Cloud (OAuth Client ID Desktop app)."
        )

    flow = InstalledAppFlow.from_client_secrets_file(
        str(client_secret_path),
        scopes=SCOPES,
    )
    creds = flow.run_local_server(port=0, prompt="consent")

    with open(client_secret_path, "r", encoding="utf-8") as f:
        client_data = json.load(f)
    installed = client_data.get("installed") or client_data.get("web") or {}

    print("\n=== COPIA QUESTI VALORI NEI SECRET GITHUB ===")
    print(f"GDRIVE_OAUTH_CLIENT_ID={installed.get('client_id', '')}")
    print(f"GDRIVE_OAUTH_CLIENT_SECRET={installed.get('client_secret', '')}")
    print(f"GDRIVE_OAUTH_REFRESH_TOKEN={creds.refresh_token or ''}")
    print("============================================\n")


if __name__ == "__main__":
    main()
