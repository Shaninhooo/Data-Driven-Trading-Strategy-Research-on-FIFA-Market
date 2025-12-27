import requests
from bs4 import BeautifulSoup
import re
import asyncio
import random
from db_utils import get_connection, insert_card, insert_card_stats, insert_card_roles, insert_card_playstyles
from futbin_scraper import load_all_hrefs, scrape_futbin_player
from scrape_live import scrape_fc26_live_prices
from requests.exceptions import SSLError, RequestException

# A smaller set of imports since not all were used in the provided snippet.

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def extract_card_id(href: str) -> int | None:
    """Extracts the unique card ID from the player's URL href."""
    # Pattern to find the number between /player/ and the next /
    match = re.search(r"/player/(\d+)/", href)
    return int(match.group(1)) if match else None

def collect_all_hrefs():
    """
    Fetches the latest promo page, compares links against the database,
    and inserts any new hrefs in bulk.
    """
    hrefs = set(load_all_hrefs())
    print(f"Loaded {len(hrefs)} existing hrefs. Sample:", list(hrefs)[:5])
    print(f"Loaded {len(hrefs)} existing hrefs.")
    new_hrefs_count = 0

    # 1. Use a context manager for the database connection for safety
    try:
        conn = get_connection()
        with conn: # The 'with conn:' handles commit/rollback/close automatically
            # Load existing hrefs from DB

            url = "https://www.futbin.com/home-tab/latest-promo"
            print(f"Fetching {url}")

            response = requests.get(url, headers=HEADERS, timeout=10)
            
            # Check for bad status codes
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")

            # Directly get all player links
            links = soup.select("a.xs-column.centered[href*='/player/']")
            print(f"Found {len(links)} card divs")

            new_entries = []

            for link in links:
                href = link["href"]
                card_id = extract_card_id(href)

                if href not in hrefs and card_id is not None:
                    hrefs.add(href)
                    new_hrefs_count += 1
                    new_entries.append((card_id, href))

            # 2. Bulk insert new hrefs into DB
            if new_entries:
                print("Count:", len(new_entries))

                # 1️⃣ Insert card IDs first
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT IGNORE INTO cards (card_id)
                        VALUES (%s)
                    """, [(cid,) for cid, _ in new_entries])
                conn.commit()  # important for FK

                # 2️⃣ Insert into meta_cards
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO meta_cards (card_id, href)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE href=VALUES(href);
                    """, new_entries)
                conn.commit()

                # 3️⃣ Insert into hrefs
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO hrefs (card_id, href)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE card_id=VALUES(card_id);
                    """, new_entries)
                conn.commit()

                print("Insertion complete.")
            else:
                print("No new hrefs found.")

            print(f"Collection complete. Found {new_hrefs_count} new hrefs.")
    
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during fetching: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


async def scrape_fc26_metadata():
    hrefs = load_all_hrefs()

    print(f"Loaded {len(hrefs)} hrefs.")

    batch_size = 50
    max_concurrency = 10
    sem = asyncio.Semaphore(max_concurrency)

    async def safe_scrape_metadata(href):
        for attempt in range(3):
            try:
                return await asyncio.to_thread(scrape_futbin_player, href)
            except (SSLError, RequestException) as e:
                wait = 2 ** attempt + random.random()
                print(f"Retry {href} due to {type(e).__name__} in {wait:.2f}s")
                await asyncio.sleep(wait)
        print(f"❌ Failed scraping metadata for {href}")
        return None

    async def process_player(href):
        async with sem:
            card_id = extract_card_id(href)
            if not card_id:
                return None

            def check_metadata_exists(card_id):
                conn = get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT 1 FROM cards
                            WHERE card_id=%s
                            AND name IS NOT NULL AND name<>''
                            AND version IS NOT NULL AND version<>''
                            LIMIT 1
                        """, (card_id,))
                        return cur.fetchone() is not None
                finally:
                    conn.close()

            exists = await asyncio.to_thread(check_metadata_exists, card_id)
            if exists:
                return (card_id, None, True)

            metadata = await safe_scrape_metadata(href)
            if not metadata:
                return None

            # Insert metadata
            await asyncio.gather(
                asyncio.to_thread(insert_card, card_id, metadata["details"], "26"),
                asyncio.to_thread(insert_card_stats, card_id, metadata["stats"]),
                asyncio.to_thread(insert_card_roles, card_id, metadata["roles"]),
                asyncio.to_thread(insert_card_playstyles, card_id, metadata["playstyles"])
            )
            print(f"✅ Inserted metadata for {card_id}")
            return (card_id, metadata, False)

    results = []
    for i in range(0, len(hrefs), batch_size):
        batch = hrefs[i:i + batch_size]
        batch_results = await asyncio.gather(*(process_player(href) for href in batch))
        results.extend([r for r in batch_results if r])
        print(f"✅ Completed metadata batch {i//batch_size + 1}")

    print("🏁 Metadata scraping done.")
    return results  # return card_id info for sales


if __name__ == "__main__":
    collect_all_hrefs()
    asyncio.run(scrape_fc26_metadata())
    asyncio.run(scrape_fc26_live_prices("all"))
    print("Finished Updating DB!")
