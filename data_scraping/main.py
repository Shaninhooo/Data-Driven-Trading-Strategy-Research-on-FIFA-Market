from futbin_scraper import scrape_fc25_players, scrape_fc26_players, collect_all_hrefs
from futgg_scraper import collect_futgg_hrefs
import asyncio
import os 

async def main():
    version = "gold_rare"
    #collect_all_hrefs("26", version)
    #collect_futgg_hrefs(version)

    # Collect Silver Sales From FutGG
    # await scrape_fc26_players_futgg(version)
    
    # Collect Gold Sales From Futbin
    await scrape_fc26_players(version)


# Using the special variable 
# __name__
if __name__=="__main__":
    asyncio.run(main())