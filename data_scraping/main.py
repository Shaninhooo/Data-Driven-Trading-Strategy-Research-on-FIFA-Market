from scraper import scrape_futbin_players, collect_all_hrefs
from db_utils import insert_card_stats, insert_card, insert_card_playstyles, insert_card_roles, add_price_to_database
import asyncio
import os 
import json

async def main():
    version = "gold_rare"
    # hrefs = collect_all_hrefs("25", "version")
    # await scrape_futbin_players("version")

    if os.path.exists(f"{version}_cards.json"):
        with open(f"{version}_cards.json", "r", encoding="utf-8") as f:
            all_cards = json.load(f)
        print(f"Loaded {len(all_cards)} cards from file.")
    else:
        all_cards = await scrape_futbin_players()
        print(f"Scraped {len(all_cards)} cards.")
    
    for card in all_cards:
        card_id = card['id']
        card_details = card['details']
        card_playstyles = card['playstyles']
        card_roles = card['roles']
        card_stats = card['stats']
        card_prices = card['price_history']

        # Insert player into DB
        insert_card(card_id, card_details)

        # Insert stats
        insert_card_stats(card_id, card_stats)

        # Insert roles/playstyles if available
        insert_card_roles(card_id, card_roles)
        insert_card_playstyles(card_id, card_playstyles)

        all_prices = []
        for platform, points in card_prices.items():
            for point in points:
                point["series_name"] = platform
                all_prices.append(point)
        add_price_to_database(card_id, all_prices)


# Using the special variable 
# __name__
if __name__=="__main__":
    asyncio.run(main())