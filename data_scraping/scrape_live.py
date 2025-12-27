import asyncio
import aiohttp
import random
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from futbin_scraper import load_new_hrefs, load_meta_hrefs, extract_card_id, load_all_hrefs
from db_utils import update_meta_cards
from deal_finder import run_deal_notifications
from update_sales import scrape_fc26_sales
import os

DB_URL = os.getenv("DB_URL")
BASE_URL = "https://www.futbin.com"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
    }


async def fetch_card_prices(href, session, sem):
    """Fetch PC + PS prices concurrently with retries, returns a dict."""
    async with sem:
        card_id = extract_card_id(href)
        if not card_id:
            return None

        results = {"card_id": card_id}

        for platform in ["pc"]:
            for attempt in range(3):  # 3 retries
                try:
                    url = f"{BASE_URL}{href}"
                    async with session.get(url, headers=get_headers(), timeout=aiohttp.ClientTimeout(total=18)) as resp:
                        if resp.status != 200:
                            if attempt == 2:
                                print(f"⚠️ HTTP {resp.status} for {platform.upper()} card {card_id}")
                            await asyncio.sleep(0.5 * (2 ** attempt))
                            continue

                        html = await resp.text()
                        soup = BeautifulSoup(html, "html.parser")
                        container = soup.find(
                            class_=f"price-box platform-{platform}-only price-box-original-player"
                        )
                        if not container:
                            if attempt == 2:
                                print(f"⚠️ No price container found for {platform.upper()} card {card_id} (possible bot detection / new layout)")

                            await asyncio.sleep(0.5 * (2 ** attempt))
                            continue

                        price_div = container.find(class_="price inline-with-icon lowest-price-1")
                        if not price_div:
                            if attempt == 2:
                                print(f"⚠️ Could not find price div for {platform.upper()} card {card_id}")
                            await asyncio.sleep(0.5 * (2 ** attempt))
                            continue

                        results[platform] = int(price_div.get_text(strip=True).replace(",", ""))
                        break  # success
                except Exception as e:
                    if attempt == 2:
                        print(f"⚠️ Failed {platform.upper()} card {card_id} on attempt {attempt+1}: {repr(e)}")
                        snippet = html[:200].replace("\n", " ") if 'html' in locals() else 'NO HTML'
                        print(f"   ↳ HTML snippet: {snippet}")

                    await asyncio.sleep(0.3 * (2 ** attempt))

        # Optional anti-bot delay
        await asyncio.sleep(random.uniform(0.1, 0.3))
        return results if "pc" in results or "ps" in results else None


async def scrape_fc26_live_prices(table="meta"):
    hrefs = load_new_hrefs() if table == "all" else load_meta_hrefs()
    total = len(hrefs)
    print(f"Loaded {total} hrefs.")

    sem = asyncio.Semaphore(6)

    engine = create_engine(
        DB_URL,
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )

    session = aiohttp.ClientSession()

    try:
        # Pre-build all tasks (fine for a few thousand cards)
        tasks = [fetch_card_prices(h, session, sem) for h in hrefs]

        batch_size = 30
        batch_updates = []

        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            result = await task
            if result:
                batch_updates.append(result)

            # Process batch
            if len(batch_updates) >= batch_size or i == total:
                await asyncio.to_thread(_update_price_batch, engine, batch_updates)
                batch_updates.clear()

            if i % 50 == 0:
                print(f"✅ Processed {i}/{total} cards")

    finally:
        await session.close()
        engine.dispose()
        import gc; gc.collect()

    # Recompute meta tables
    await asyncio.to_thread(update_meta_cards)
    print("🏁 Live price update completed.")


def _update_price_batch(engine, updates):
    """Runs inside a thread → updates DB efficiently in bulk."""
    if not updates:
        return

    rows = [
        {"cid": r["card_id"], "price": r["pc"]}
        for r in updates
        if "pc" in r
    ]

    if not rows:
        return

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE cards SET current_pc_price=:price WHERE card_id=:cid"),
            rows  # executemany
        )


async def main():
    print("🔹 Starting live price scraping...")
    await scrape_fc26_live_prices(table="meta")
    print("🔹 Live prices done. Running deal notifications...")
    await run_deal_notifications()
    print("🔹 Notifications sent. Running sales scraping...")
    await scrape_fc26_sales()
    print("🏁 All tasks completed.")


if __name__ == "__main__":
    import time
    start_time = time.time()
    asyncio.run(main())
    print(f"Total runtime: {time.time() - start_time:.2f}s")
