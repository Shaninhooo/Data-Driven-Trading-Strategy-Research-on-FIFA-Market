from futbin_scraper import scrape_fc26_players, collect_all_hrefs
# from futgg_scraper import collect_futgg_hrefs
from db_utils import initcardTable
import asyncio

async def main():

    # Init Tables
    initcardTable()
    # collect_futgg_hrefs(version)

    # Collect Silver Sales From FutGG
    # await scrape_fc26_players_futgg(version)
    
    # Collect Hrefs From Futbin
  
    # Create tasks for all versions
    versions = ["gold_rare", "icons", "heroes", "gold_if"]
    for version in versions:
        collect_all_hrefs(version)  # synchronous
        await scrape_fc26_players(version)  # async
    
    print("Finished Scraping Process!")


# Using the special variable 
# __name__
if __name__=="__main__":
    asyncio.run(main())