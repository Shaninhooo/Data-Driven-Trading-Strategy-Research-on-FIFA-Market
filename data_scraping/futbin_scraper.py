import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import re
from db_utils import get_connection

BASE_URL = "https://www.futbin.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def extract_card_id(href: str) -> int | None:
    """Safely extract card_id from href. Returns None if invalid."""
    match = re.search(r"/player/(\d+)/?", href)
    if match:
        return int(match.group(1))
    else:
        print(f"Warning: Could not extract card_id from href: {href}")
        return None

def collect_all_hrefs(version):
    hrefs = set()
    new_hrefs = 0
    conn = get_connection()

    # Load existing hrefs from DB
    with conn.cursor() as cur:
        cur.execute("SELECT href FROM hrefs WHERE version=%s", (version,))
        for row in cur.fetchall():
            hrefs.add(row['href']) 

    page_num = 1

    while True:
        url = f"{BASE_URL}/26/players?page={page_num}&version={version}"
        print(f"[Page {page_num}] Fetching {url}")

        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch page {page_num}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("tr", class_="player-row")

        # Stop only when page has no rows at all
        if not rows:
            print(f"No player rows found, stopping at page {page_num}")
            break

        page_new_hrefs = 0
        new_entries = []

        for row in rows:
            name_tag = row.find("a", class_="table-player-name")
            if name_tag and "href" in name_tag.attrs:
                href = name_tag["href"]
                card_id = extract_card_id(href)
                version_detail = row.find("div", class_="table-player-revision")
                price = row.find("div", class_="price")
                if "SBC" in version_detail.get_text():
                    continue
                if price:
                    price_val = price.get_text(strip=True).replace(",", "")
                    if price_val == "0":
                        continue
                else:
                    continue

                if href not in hrefs:
                    hrefs.add(href)
                    page_new_hrefs += 1
                    new_hrefs += 1
                    new_entries.append((card_id, href, version))

        # Bulk insert new hrefs into DB
        if new_entries:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO hrefs (card_id, href, version)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE card_id=card_id;
                """, new_entries)
            conn.commit()

        print(f"Page {page_num}: collected {page_new_hrefs} new hrefs")
        page_num += 1

    print(f"Collected {new_hrefs} new hrefs in total.")
    conn.close()
    return list(hrefs)

def load_all_hrefs():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT href FROM hrefs")
            rows = cur.fetchall()
            return [row['href'] for row in rows]  # skip invalid
    finally:
        conn.close()


def load_version_hrefs(version):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT h.href
                FROM hrefs h
                WHERE h.version = %s
            """, (version,))  # note the comma to make it a tuple
            
            rows = cur.fetchall()
            return [row['href'] for row in rows]  # default cursor returns tuples
    finally:
        conn.close()

def load_new_hrefs():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT h.href
                FROM hrefs h
                JOIN cards c ON c.card_id = h.card_id
                WHERE c.current_pc_price IS NULL
                   OR c.current_pc_price = 0
            """)
            
            return [row["href"] for row in cur.fetchall()]
    finally:
        conn.close()


def load_meta_hrefs():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT href FROM meta_cards")
            rows = cur.fetchall()
            # rows = [('/26/player/31/mohamed-salah',), ...]
            return [row['href'] for row in rows]
    finally:
        conn.close()




def normalize_column(stat_name: str) -> str:
    """
    Normalize stat name:
    - Remove special characters
    - Replace spaces with underscores
    - Convert to lowercase
    """
    stat_name = re.sub(r'[^A-Za-z0-9 ]+', '', stat_name)
    stat_name = stat_name.replace(" ", "_").lower()
    return stat_name

# Scrapes Specific Futbin Player Metadata
def scrape_futbin_player(href):

    url = f"https://www.futbin.com{href}"
    response = requests.get(url, headers=HEADERS)
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
    rating_tag = player_card.select_one("div.playercard-26-rating")
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




