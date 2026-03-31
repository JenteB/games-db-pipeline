import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
from supabase import create_client
import datetime

# --- 1. CONFIGURATIE (Vul je eigen gegevens in) ---
SUPABASE_URL = "https://zhyzoxwrotyeaogeewjd.supabase.co"
SUPABASE_KEY = "sb_publishable_oUSwvRMa_-aSZT-k5P8RmQ_ljyHnu42" # Gebruik de key uit je Colab script

def run_scraper():
    base_url = "https://thegamesdb.net/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BI-Project-Scanner"}
    all_games_data = []

    print("🔍 Bezig met het verzamelen van de nieuwste games van TheGamesDB...")

    # --- 2. EXTRACTIE (Jouw code) ---
    response = requests.get(base_url, headers=headers, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")
    game_cards = soup.select("div.row[style*='padding-bottom:10px']")

    # We pakken de eerste 10-15 games om te voorkomen dat GitHub acties te lang duren
    for index, card in enumerate(game_cards[:15]):
        title_node = card.select_one("h4 a")
        if not title_node: continue

        title = title_node.get_text(strip=True)
        detail_url = base_url + title_node.get("href")

        platform_node = card.select_one("h6.text-muted")
        platform_raw = platform_node.get_text(strip=True).replace("Platform: ", "") if platform_node else "Onbekend"

        platform_type = "Console"
        if "PC" in platform_raw or "Windows" in platform_raw:
            platform_type = "PC"
        elif "Arcade" in platform_raw:
            platform_type = "Arcade"

        try:
            res_detail = requests.get(detail_url, headers=headers, timeout=15)
            soup_detail = BeautifulSoup(res_detail.text, "html.parser")

            genre = "Onbekend"
            page_text = soup_detail.get_text()
            if "Genre(s):" in page_text:
                genre = page_text.split("Genre(s):")[1].split("\n")[0].strip()

            all_games_data.append({
                "title": title,
                "platform": platform_raw,
                "platform_group": platform_type,
                "genre_raw": genre
            })
            print(f"➡️ Verwerkt: {title}")

        except Exception as e:
            print(f"⚠️ Fout bij {title}: {e}")

        time.sleep(1)

    # --- 3. TRANSFORMATIE (Jouw opschoon-stappen) ---
    if not all_games_data:
        print("Geen data gevonden.")
        return

    df_raw = pd.DataFrame(all_games_data)
    df_clean = df_raw.assign(genre=df_raw['genre_raw'].str.split(' | ')).explode('genre')
    df_clean['genre'] = df_clean['genre'].str.replace('|', '').str.strip()
    df_clean = df_clean[df_clean['genre'] != ""]

    # Voeg een tijdstempel toe voor het bewijs van automatisering
    scraped_at = datetime.datetime.now().isoformat()

    # --- 4. LOADING (Data naar Supabase sturen) ---
    rows_to_insert = []
    for _, row in df_clean.iterrows():
        rows_to_insert.append({
            "title": row['title'],
            "platform": row['platform'],
            "platform_group": row['platform_group'],
            "genre": row['genre'],
            "scraped_at": scraped_at
        })

    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        supabase.table("games_tracker").insert(rows_to_insert).execute()
        print(f"✅ SUCCES: {len(rows_to_insert)} rijen gepusht naar Supabase op {scraped_at}")
    except Exception as e:
        print(f"❌ Supabase Fout: {e}")

if __name__ == "__main__":
    run_scraper()