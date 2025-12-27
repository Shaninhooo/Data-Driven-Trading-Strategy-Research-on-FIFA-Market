from futbin_scraper import extract_card_id, load_meta_hrefs
# from futgg_scraper import collect_futgg_hrefs
from db_utils import async_insert_batch_sales, get_connection
import asyncio
from bs4 import BeautifulSoup
import datetime
import pytz
import aiohttp

BASE_URL = "https://www.futbin.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# Scrape Live Hourly Prices
async def fetch_sales(session, url):
    """Fetch page content asynchronously."""
    async with session.get(url, headers=HEADERS) as resp:
        return await resp.text()


def parse_sales(html, latest_sale_time=None):
    """
    Parse the sales table HTML, stopping if a row is older than latest_sale_time.
    latest_sale_time should be a timezone-aware datetime or None.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        sales_table = soup.find("tbody")
        if not sales_table:
            return []

        sales_data = []
        uk = pytz.timezone("Europe/London")
        adelaide = pytz.timezone("Australia/Adelaide")
        cutoff = adelaide.localize(datetime.datetime(2024, 1, 1))
        current_year = datetime.datetime.now().year

        for row in sales_table.find_all("tr"):
            cols = row.find_all("td")
            if not cols:
                continue

            # Parse date/time
            date_span = cols[0].find("span", class_="sales-date-time")
            sale_time_str = date_span.get_text(strip=True) if date_span else None
            if sale_time_str:
                naive_dt = datetime.datetime.strptime(sale_time_str, "%b %d, %I:%M %p")
                naive_dt = naive_dt.replace(year=current_year)
                uk_dt = uk.localize(naive_dt)
                adelaide_dt = uk_dt.astimezone(adelaide)
            else:
                adelaide_dt = None

            # Stop if this sale is older than latest in DB
            if latest_sale_time and adelaide_dt <= latest_sale_time:
                break

            # Parse prices
            price = int(cols[1].get_text(strip=True).replace(",", "")) if len(cols) > 1 else None
            try:
                sold_price_text = cols[2].get_text(strip=True) if len(cols) > 2 else "0"
                sold_price = int(sold_price_text.replace(",", ""))
            except ValueError:
                sold_price = 0

            # Sale type
            type_div = cols[5].find("div", class_="inline-popup-content") if len(cols) > 5 else None
            sale_type = type_div.get_text(strip=True) if type_div else None

            if adelaide_dt and adelaide_dt >= cutoff:
                sales_data.append({
                    "sale_time": adelaide_dt,
                    "listed_price": price,
                    "sold_price": sold_price,
                    "sale_type": sale_type
                })

        return sales_data

    except Exception as e:
        print(f"Error parsing sales: {e}")
        return []


async def get_sales(sales_href, session, card_id):
    """
    Fetch sales for all platforms using a single aiohttp session.
    Stops parsing rows older than the latest sale in DB.
    Returns a dict {platform: [sales]}.
    """
    platforms = ["pc", "ps"]

    # 1️⃣ Fetch the latest sale time per platform from DB
    def fetch_latest_sale_time(card_id, platform):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(sale_time) as latest
                    FROM market_sales
                    WHERE card_id=%s AND platform=%s
                """, (card_id, platform))
                row = cur.fetchone()
                latest = row['latest'] if row and row['latest'] else None
                if latest and latest.tzinfo is None:
                    # Make it aware in Adelaide timezone
                    adelaide = pytz.timezone("Australia/Adelaide")
                    latest = adelaide.localize(latest)
                return latest
        finally:
            conn.close()

    latest_sale_times = {}
    for platform in platforms:
        latest_sale_times[platform] = await asyncio.to_thread(fetch_latest_sale_time, card_id, platform)

    # 2️⃣ Fetch sales pages concurrently
    tasks = [
        fetch_sales(session, f"{BASE_URL}{sales_href}?platform={platform}")
        for platform in platforms
    ]
    html_results = await asyncio.gather(*tasks, return_exceptions=True)

    # 3️⃣ Parse sales with early stopping
    sales_by_platform = {}
    for platform, html in zip(platforms, html_results):
        if isinstance(html, str):
            sales_by_platform[platform] = parse_sales(html, latest_sale_times[platform])
        else:
            sales_by_platform[platform] = []

    return sales_by_platform

async def scrape_fc26_sales():
    hrefs = load_meta_hrefs()
    total = len(hrefs)
    print(f"Loaded {total} hrefs.")

    sem = asyncio.Semaphore(10)  # max 10 players concurrently
    batch_size = 50

    async with aiohttp.ClientSession() as session:  # create one session
        async def process_player(href):
            async with sem:
                card_id = extract_card_id(href)
                if not card_id:
                    return []

                sales_href = href.replace("player", "sales")
                sales = await get_sales(sales_href, session, card_id)  # pass session

                all_sales = []
                for platform, s_list in sales.items():
                    for sale in s_list:
                        sale["card_id"] = card_id
                        sale["platform"] = platform
                        all_sales.append(sale)

                return all_sales

        all_sales_to_insert = []

        for i in range(0, len(hrefs), batch_size):
            batch = hrefs[i:i + batch_size]
            results = await asyncio.gather(*(process_player(href) for href in batch))
            # Flatten results
            for player_sales in results:
                all_sales_to_insert.extend(player_sales)
            # Insert batch into DB
            await async_insert_batch_sales(all_sales_to_insert)
            all_sales_to_insert.clear()
            print(f"✅ Completed batch {i//batch_size + 1}/{(total+batch_size-1)//batch_size}")

    print("🏁 Sales scraping done.")

async def main():
    await scrape_fc26_sales()  # async
    
    print("Finished Scraping Process!")


# Using the special variable 
# __name__
if __name__=="__main__":
    asyncio.run(main())