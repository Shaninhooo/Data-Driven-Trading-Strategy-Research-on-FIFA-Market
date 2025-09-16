from scraper import scrape_futbin_players, collect_all_hrefs
from db_utils import insert_card_stats, insert_card, insert_card_playstyles, insert_card_roles, addPricetoDatabase
import asyncio

async def main():
    hrefs = await collect_all_hrefs("25", "gold_rare")
    all_cards = await scrape_futbin_players("25", "gold_rare")
    
    for card in all_cards:
        card_id = card['id']
        card_details = card['details']
        card_playstyles = card['playstyles']
        card_roles = card['roles']
        card_stats = card['stats']
        card_prices = card['prices']

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
        addPricetoDatabase(card_id, all_prices)


# Using the special variable 
# __name__
if __name__=="__main__":
    asyncio.run(main())