import asyncio
from pyppeteer import launch
import requests
from bs4 import BeautifulSoup
import datetime
import random
from collections import defaultdict
from unidecode import unidecode



async def get_all_prices(pc_series, console_series):
    pc_prices = await get_prices(page, pc_url)
    console_prices = await get_prices(page, console_url)
    crossplay_prices = await get_prices(page, crossplay_url)
    """
    pc_series and console_series: list of dicts like {'x': datetime, 'y': value}
    Returns: list of dicts with {'x': datetime, 'pc': value, 'console': value}
    """
    merged = defaultdict(dict)

    for point in pc_series:
        merged[point['x']]['pc'] = point['y']

    for point in console_series:
        merged[point['x']]['console'] = point['y']

    # Fill missing values with None
    result = []
    for timestamp, vals in merged.items():
        result.append({
            'x': timestamp,
            'pc': vals.get('pc'),
            'console': vals.get('console')
        })

    # Optional: sort by timestamp
    result.sort(key=lambda d: d['x'])
    return result

async def get_prices(page, url):
    await page.goto(url, {"waitUntil": "networkidle2"})
    
    chart_data = await page.evaluate('''() => {
        let data = []
        Highcharts.charts.forEach((chart, index) => {
            chart.series.forEach(series => {
                let series_data = series.data.map(point => ({
                    x: point.x,
                    y: point.y,
                    series_name: series.name
                }));
                data.push(...series_data);
            });
        });
        return data;
    }''')

    # Convert timestamps to datetime in Python
    for item in chart_data:
        item['x'] = datetime.datetime.fromtimestamp(item['x'] / 1000)
    
    return chart_data



    

def scrape_futbin_players(base_url):
    # browser = await launch(headless=True, executablePath=r"C:\Program Files\Google\Chrome\Application\chrome.exe")  # or your path
    session = requests.Session()

    all_players = []
    pageNum = 1

    while True:
        url = f"{base_url}players?page={pageNum}&version=gold_rare"
        response = session.get(url)
        html_content = response.text  # <-- this is HTML, not JSON
        soup = BeautifulSoup(html_content, "html.parser")
        player_rows = soup.find_all("tr", class_="player-row")

        if response.status_code != 200:
             break  # stop if page not found or error
        
        player_rows = soup.find_all("tr", class_="player-row")

        if not player_rows:
            break

        for player in player_rows:
            name_tag = player.find("a", class_="table-player-name")
            if name_tag:
                all_players.append(scrape_futbin_player(name_tag['href']))  # coroutine, not awaited yet


        pageNum += 1

# Scrapes Specific Futbin Player
def scrape_futbin_player(href):

    url = f"https://www.futbin.com{href}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch {href}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    player_info_box = soup.find("div", class_="player-header-info-box")
    player_card = soup.find("div", class_="playercard-l")

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
    rating = rating_tag.text.strip() if rating_tag else None

    # Get Player Position
    positions_tag = player_card.select_one("div[class*='position']")
    positions = positions_tag.text.strip() if positions_tag else None

    # Get Player Info

    club_tag = player_info_box.select_one("img[alt*='Club']")
    club = unidecode(club_tag['title'])

    nation_tag = player_info_box.select_one("img[alt*='Nation']")
    nation = unidecode(nation_tag['title'])

    league_tag = player_info_box.select_one("img[alt*='League']")
    league = unidecode(league_tag['title'])

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
                "Playstyle": playstyle_name,
                "Plus": is_plus
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
                "Position": position,
                "Role": role_name,
                "Plus": strength
            })

    
    # Extract Player Stats
    stats_categories = {
        "Pace": "1",
        "Shooting": "2",
        "Passing": "3",
        "Dribbling": "4",
        "Defending": "5",
        "Physical": "6"
    }

    all_stats = {}

    for category, stat_id in stats_categories.items():
        wrapper = soup.find("div", {"data-base-stat-id": stat_id})
        values = {}
        if wrapper:
            for stat_div in wrapper.select(".player-stat-value"):
                stat_name_div = stat_div.find_previous("div", class_="player-stat-name")
                stat_name = stat_name_div.text.strip() if stat_name_div else "Unknown"
                stat_value = stat_div.get("data-stat-value", None)
                values[stat_name] = stat_value
        all_stats[category] = values

    player_data = {
        "Name": name,
        "Rating": rating,
        "Positions": positions,
        "Club": club,
        "Nation": nation,
        "League": league,
        "Playstyles": playstyles,
        "Roles": roles,
        "Stats": all_stats
    }

    # Print details
    print(f"\n=== {name} ===")
    print(f"Rating: {rating}, Positions: {positions}")
    print(f"Club: {club}, Nation: {nation}, League: {league}")
    print("Playstyles:")
    for ps in playstyles:
        print(f" - {ps['Playstyle']} {'+' if ps['Plus'] else ''}")
    print("Roles:")
    for role in roles:
        print(f" - {role['Position']}: {role['Role']} {'+'*role['Plus']}")
    print("Stats:")
    for cat, stats in all_stats.items():
        print(f" {cat}:")
        for k, v in stats.items():
            print(f"   {k}: {v}")
    
    return player_data

players = scrape_futbin_players("https://www.futbin.com/25/")
