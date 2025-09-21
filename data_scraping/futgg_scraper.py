import asyncio
import requests
from bs4 import BeautifulSoup
import datetime
import random
from collections import defaultdict
from unidecode import unidecode
import re
import pytz
import aiohttp
import psutil
import json
import os
from db_utils import insert_card_stats, insert_card, insert_card_playstyles, insert_card_roles, add_price_to_database, async_insert_sale_db

BASE_URL = "https://www.fut.gg"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

version_ids = {
    "gold": 1,
    "gold_rare": 2,
    "gold_common": 3,
    "silver": 4,
    "silver_rare": 5,
    "silver_common": 6
}


# Main

def collect_futgg_hrefs(version):
    hrefs = set()
    href_lines = 0
    # Load existing hrefs from file
    if os.path.exists(f"data_scraping/futgg_hrefs/{version}_hrefs.txt"):
        with open(f"data_scraping/futgg_hrefs/{version}_hrefs.txt", "r", encoding="utf-8") as f:
            for line in f:
                hrefs.add(line.strip())
                href_lines += 1

    completed_pages = href_lines // 30  # integer division
    page_num = 1 + completed_pages
    new_hrefs = 0

    while True:
        url = f"{BASE_URL}/players/?page={page_num}&quality_id=[{version_ids[version]}]"
        print(f"[Page {page_num}] Fetching {url}")

        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch page {page_num}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        players = soup.find_all("a", class_="group/player")

        if not players:
            print(f"No player rows found, stopping at page {page_num}")
            break

        page_new_hrefs = 0
        for player in players:
            if player and "href" in player.attrs:
                href = player["href"]
                if href not in hrefs:
                    hrefs.add(href)
                    page_new_hrefs += 1
                    new_hrefs += 1

        if page_new_hrefs == 0:
            print(f"No new hrefs found on page {page_num}, stopping.")
            break

        print(f"Page {page_num}: collected {page_new_hrefs} new hrefs")

        page_num += 1

    # Save all hrefs back to file
    with open(f"data_scraping/futgg_hrefs/{version}_hrefs.txt", "w", encoding="utf-8") as f:
        for href in sorted(hrefs):  # optional: sort to keep order consistent
            f.write(href + "\n")

    print(f"Collected {new_hrefs} new hrefs in total.")
    return list(hrefs)


def load_hrefs(version):
    """Load hrefs from hrefs.txt if it exists."""
    hrefs = []
    if os.path.exists(f"data_scraping/futgg_hrefs/{version}_hrefs.txt"):
        with open(f"data_scraping/futgg_hrefs/{version}_hrefs.txt", "r", encoding="utf-8") as f:
            hrefs = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(hrefs)} {version} hrefs from file.")
    return hrefs

async def scrape_fc26_players(version):

    # Load hrefs
    hrefs = load_hrefs("26", version)
    print(f"Loaded {len(hrefs)} futgg hrefs")
    results = []

    sem = asyncio.Semaphore(5)  # concurrency limit

    async def process_player(href):
        async with sem:
            await asyncio.sleep(random.uniform(0.5,2))
            metadata = scrape_futgg_player(href)
            if not metadata: return None
            card_id = metadata["id"]
            sales_href = href.replace("player", "sales")
            sales = await get_sales(sales_href)
            result = {
                "card_id": card_id,
                "details": metadata["details"],
                "playstyles": metadata["playstyles"],
                "roles": metadata["roles"],
                "stats": metadata["stats"],
                "market_sales": sales
            }
            print(f"Scraped {metadata['details']['name']}")
            return result

    tasks = [process_player(href) for href in hrefs]
    for coro in asyncio.as_completed(tasks):
        res = await coro
        if res is None:
            print("Skipped a player because scraping returned None")
            continue  # skip this iteration
        
        res_card_id =  res["card_id"]
        
        # Insert Scraped Data to Database 
        # Insert player into DB
        insert_card(res_card_id, res["details"], "26")
        # Insert stats
        insert_card_stats(res_card_id, res["stats"])
        # Insert roles/playstyles if available
        insert_card_roles(res_card_id, res["roles"])
        insert_card_playstyles(res_card_id, res["playstyles"])

        all_prices = []
        for platform, sales in res["market_sales"].items():
            for sale in sales:
                sale["platform"] = platform
                all_prices.append(sale)
        await async_insert_sale_db(res_card_id, all_prices)
    
    return


# Player Details

def scrape_futbin_player(href):

    url = f"https://www.futbin.com{href}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch {href}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    player_info_box = soup.find("div", class_="player-header-info-box")
    player_card = soup.find("div", class_="playercard-l")

    card_id = int(href.split("/")[3])

    # Get Player Name
    if player_card:
        # Try multiple ways to get name
        if player_card.has_attr("title") and player_card["title"]:
            name = unidecode(player_card["title"].strip())
        else:
            name_div = player_card.find("div", class_="player-name")
            if name_div and name_div.text:
                name = unidecode(name_div.text.strip())

    # Get Player Rating
    rating_tag = player_card.select_one("div[class*='rating']")
    if rating_tag:
        rating_text = rating_tag.get_text(strip=True)
        # extract only digits
        match = re.search(r"\d+", rating_text)
        rating = int(match.group()) if match else None



    # Get Player Position
    position_tag = player_card.select_one("div[class*='position']")
    position = position_tag.text.strip() if position_tag else None

    # Get Player Info

    club_tag = player_info_box.select_one("img[alt*='Club']")
    club = unidecode(club_tag['title'])

    nation_tag = player_info_box.select_one("img[alt*='Nation']")
    nation = unidecode(nation_tag['title'])

    league_tag = player_info_box.select_one("img[alt*='League']")
    league = unidecode(league_tag['title'])

    version_tag = soup.select_one("a[href*='version='] span.text-ellipsis")
    version = version_tag.get_text(strip=True) if version_tag else None

    player_info_grid = soup.find("div", class_="player-info-box-player-info-grid")
    rows = player_info_grid.find_all("div", class_="xxs-row xs-font align-center")

    weakfoot = rows[0].find_all("div")[1].get_text(strip=True)

    skills =  rows[1].find_all("div")[1].get_text(strip=True)

    height_text =  rows[2].find_all("div")[1].get_text(strip=True)
    height = int(re.search(r'\d+', height_text).group())

    # Get Player PS and Roles
    playstyle_wrapper = soup.find("div", class_="player-abilities-wrapper")
    playstyles = []

    if playstyle_wrapper:
        # Only look inside this wrapper
        playstyle_tags = playstyle_wrapper.find_all("a", class_="playStyle-table-icon")

        for tag in playstyle_tags:
            if tag.find_parent(class_="hidden"):
                continue
            name_div = tag.find("div")
            playstyle_name = name_div.text.strip() if name_div else None
            
            classes = tag.get("class", [])
            is_plus = "psplus" in classes
            
            playstyles.append({
                "playstyle": playstyle_name,
                "plus": is_plus
            })
    
    roles = []

    role_boxes = soup.select(".player-roles-wrapper .xxs-row.align-center")
    for box in role_boxes:
        if box.find_parent(class_="hidden"):
            continue
        # position (ST, LW, etc.)
        position_tag = box.find("div", class_="xs-font uppercase text-faded")
        position = position_tag.text.strip() if position_tag else None

        # role name and plus strength
        role_tag = box.find("a")
        if role_tag:
            # Everything before the nested <div> is the role name
            role_name = role_tag.contents[0].strip()

            # The nested <div> contains +, ++, etc.
            plus_tag = role_tag.find("div")
            strength = plus_tag.text.count("+") if plus_tag else 0
        
            roles.append({
                "position": position,
                "role": role_name,
                "plus": strength
            })
    
    # Extract Accelerate
    accelerate_tag = soup.select_one("a.accelerate-bar:not(.hidden) .player-accelerate-text")

    if accelerate_tag:
        accelerate_text = accelerate_tag.get_text(" ", strip=True)
        match = re.search(r'\b(?:Explosive|Controlled|Lengthy)\b', accelerate_text, re.I)
        accelerate = match.group(0).capitalize() if match else None
    else:
        accelerate = None

    
    # Extract Player Stats
    stats_categories = {
        "pace": "1",
        "shooting": "2",
        "passing": "3",
        "dribbling": "4",
        "defending": "5",
        "physical": "6"
    }

    all_stats = {}

    for category, stat_id in stats_categories.items():
        wrapper = soup.find("div", {"data-base-stat-id": stat_id})
        values = {}
        if wrapper:
            for stat_div in wrapper.select(".player-stat-value"):
                stat_name_div = stat_div.find_previous("div", class_="player-stat-name")
                stat_name = stat_name_div.text.strip() if stat_name_div else "Unknown"

                stat_name = normalize_column(stat_name)
                normalized_category = normalize_column(category)
                if stat_name == normalized_category:
                    stat_name = f"{stat_name}_overall"
                stat_value = stat_div.get("data-stat-value", None)
                values[stat_name] = stat_value
        all_stats[category] = values

    player_details = {
        "name": name,
        "rating": rating,
        "position": position,
        "version": version,
        "club": club,
        "nation": nation,
        "league": league,
        "weakfoot": weakfoot,
        "skills": skills,
        "height": height,
        "accelerate": accelerate
    }
    
    return {
        "id": card_id,
        "details": player_details,
        "playstyles": playstyles,
        "roles": roles,
        "stats": all_stats
    }

# Sales

# Scrape Live Hourly Prices
async def fetch_sales(session, url):
    """Fetch page content asynchronously."""
    async with session.get(url, headers=HEADERS) as resp:
        return await resp.text()



def parse_sales(html):
    try:
        soup = BeautifulSoup(html, "html.parser")
        sales_table = soup.find("tbody")
        if sales_table is None:
            print(f"No sales table found")
            return []  # or handle gracefully

        sales_data = []

        for row in sales_table.find_all("tr"):
            cols = row.find_all("td")

            # Get date/time
            date_span = cols[0].find("span", class_="sales-date-time")
            sale_time_str = date_span.get_text(strip=True) if date_span else None

            # Parse string into datetime
            if sale_time_str:
                sale_time = datetime.datetime.strptime(sale_time_str, "%b %d, %I:%M %p")
                # Add current year if needed
                sale_time = sale_time.replace(year=datetime.datetime.now().year)
            else:
                sale_time = None

            # Get price (assuming second td)
            price_text = cols[1].get_text(strip=True)
            price = int(price_text.replace(",", "")) if price_text else None
            

            # Other columns (optional)
            sold_price_text = cols[2].get_text(strip=True)
            try:
                sold_price = int(sold_price_text.replace(",", "")) if sold_price_text else 0
            except ValueError:
                sold_price = 0

            # Extract Bid / other text from last column
            type_div = cols[5].find("div", class_="inline-popup-content")
            sale_type = type_div.get_text(strip=True) if type_div else None

            sales_data.append({
                "sale_time": sale_time,
                "listed_price": sold_price,
                "sold_price": sold_price,
                "sale_type": "Buy Now"
            })

        uk = pytz.timezone("Europe/London")
        adelaide = pytz.timezone("Australia/Adelaide")
        cutoff = datetime.datetime(2024, 1, 1, tzinfo=adelaide)

        filtered_data = []
        for item in sales_data:
            if item['sale_time']:
                # Localize as UK time
                uk_dt = uk.localize(item['sale_time'])
                # Convert to Adelaide
                adelaide_dt = uk_dt.astimezone(adelaide)
                if adelaide_dt >= cutoff:
                    item['sale_time'] = adelaide_dt.isoformat()
                    filtered_data.append(item)

        sales_data = filtered_data
    except Exception as e:
        print(f"Error: {e}")

    return sales_data


async def get_sales(sales_href):
    platforms = ["pc", "ps"]
    async with aiohttp.ClientSession() as session:
        tasks = []
        for platform in platforms:
            url = f"{BASE_URL}{sales_href}?platform={platform}"
            tasks.append(fetch_sales(session, url))

        html_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Parse HTML for each platform
    sales_by_platform = {}
    for platform, html in zip(platforms, html_results):
        if isinstance(html, str):
            sales_by_platform[platform] = parse_sales(html)
        else:
            sales_by_platform[platform] = []

    return sales_by_platform