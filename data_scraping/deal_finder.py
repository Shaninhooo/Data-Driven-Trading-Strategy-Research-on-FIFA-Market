import pandas as pd
import numpy as np
from discordwebhook import Discord
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import discord
from discord.ext import commands, tasks
import asyncio
from datetime import timedelta

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True  # Required to read messages
bot = commands.Bot(command_prefix="!", intents=intents)

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = 1421417739213471795

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    
    channel = bot.get_channel(CHANNEL_ID)
    
    if channel:
        # Only delete messages that are not pinned
        deleted = await channel.purge(limit=1000, check=lambda m: not m.pinned)
        print(f"Deleted {len(deleted)} messages (pinned messages preserved)")
    
    await bot.close()

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
discord = Discord(url=DISCORD_WEBHOOK)
DB_URL = os.getenv("DB_URL")  # e.g., "mysql+pymysql://user:pass@host/dbname"


def send_discord_message(message: str):
    if not DISCORD_WEBHOOK:
        print("⚠️ No Discord webhook set.")
        return
    discord.post(content=message)



# ------------------- DATA FETCHING -------------------

def fetch_data(conn, platform="pc"):
    """Fetch raw sales in last 8 hours for dip detection"""
    query = f"""
        SELECT 
            ms.card_id,
            c.name,
            c.version,
            ms.sale_time,
            ms.sold_price,
            ms.platform
        FROM market_sales ms
        JOIN cards c ON ms.card_id = c.card_id
        WHERE ms.sold_price > 20000
          AND ms.platform = '{platform}'
          AND ms.sale_time >= NOW() - INTERVAL 49 HOUR
    """
    return pd.read_sql(query, conn)


def strategy_discord_notify(conn, strategy, strategy_name):
    """
    Discord-ready notification system
    - Runs a given strategy
    - Outputs strategy-specific formatted messages
    """
    platforms = ["pc", "ps"]
    
    for plat in platforms:
        df = fetch_data(conn, platform=plat)
        if df.empty:
            print(f"No data on {plat} for {strategy_name}")
            continue

        df_sorted = df.sort_values("sale_time")
        candidates = strategy(df_sorted)
            
        for idx, row in candidates.iterrows():
            buy_price = row['buy_price']
            z_score_potential_profit = int(row['long_mean']* 0.95)  - buy_price
            z_score_profit_margin_pct = z_score_potential_profit / buy_price * 100
            drop_pct = (row['long_mean'] - buy_price) / row['long_mean'] * 100

            # different template depending on strategy
            if strategy_name == "zscore":
                msg = (
                    f"📊 **Z-Score Strategy Opportunity**\n"
                    f"🌐 {plat.upper()}\n"
                    f"🎴 {row['name']} ({row['score']})\n"
                    f"Drop: {drop_pct:.2f}% | Buy ~ {int(buy_price)}\n"
                    f"Long Average: {int(row['long_mean'])}\n"
                    f"💰 Profit: {int(z_score_potential_profit)} ({z_score_profit_margin_pct:.2f}%)\n"
                )

            send_discord_message(msg)
            
       

# ------------------- STRATEGIES -------------------
    
    
def calculate_rebound_flag(x: pd.Series, window: int = 10) -> bool:
    """
    Checks if the last 'window' prices show a net upward momentum (e.g., 
    more than 50% of the steps were increases).
    """
    if len(x) < window:
        return False
    
    # Slice the last 'window' prices
    y = x.tail(window).values 
    
    # Count how many steps were increases (y[i] < y[i+1])
    # Returns True if at least window/2 + 1 steps were increases
    increase_count = sum(y[i] < y[i+1] for i in range(len(y) - 1))
    
    # We require a majority of steps to be increasing for a flexible rebound
    required_increases = int((window - 1) / 2) + 1
    
    return increase_count >= required_increases

# def live_mean_reversion_strategy(market_prices_df, lookback_short=4, lookback_long=25):

#     # 1. Sort by time
#     df_sorted = market_prices_df.sort_values('sale_time')

#     latest_time = df_sorted['sale_time'].max()
#     long_cutoff_time = latest_time - timedelta(hours=lookback_long)
#     short_cutoff_time = latest_time - timedelta(hours=lookback_short)

#     short_df = df_sorted[df_sorted['sale_time'] >= short_cutoff_time]
#     long_df = df_sorted[df_sorted['sale_time'] >= long_cutoff_time]
    
#     # 2. Group by card_id

#     short_mean_series = short_df.groupby('card_id')['sold_price'].mean()
#     rolling_min_short_series = short_df.groupby('card_id')['sold_price'].min()
#     rolling_max_short_series = short_df.groupby('card_id')['sold_price'].max()
#     rolling_min_long_series = long_df.groupby('card_id')['sold_price'].min()
#     long_mean_series = long_df.groupby('card_id')['sold_price'].mean()
#     std_dev_series = long_df.groupby('card_id')['sold_price'].std()
#     liquid_series = short_df.groupby('card_id')['sold_price'].count()

#     valid_card_ids = short_mean_series.index.intersection(long_mean_series.index)
#     latest_data = df_sorted.groupby('card_id').tail(1).copy()
#     latest_data = latest_data[latest_data['card_id'].isin(valid_card_ids)].copy()

    
#     # 4. Flexible rebound detection

    
#     rebound_series = short_df.groupby('card_id')['sold_price'].apply(
#         lambda x: calculate_rebound_flag(x, window=lookback_short)
#     )
    
#     latest_data['short_mean'] = latest_data['card_id'].map(short_mean_series)
#     latest_data['long_mean'] = latest_data['card_id'].map(long_mean_series)
#     latest_data['std_dev'] = latest_data['card_id'].map(std_dev_series)
#     latest_data['rebound_flag'] = latest_data['card_id'].map(rebound_series)
#     latest_data['liquid'] = latest_data['card_id'].map(liquid_series)
#     latest_data['average_price'] = latest_data['card_id'].map(short_mean_series)
#     latest_data['rolling_min_short'] = latest_data['card_id'].map(rolling_min_short_series)
#     latest_data['rolling_max_short'] = latest_data['card_id'].map(rolling_max_short_series)
#     latest_data['rolling_min_long'] = latest_data['card_id'].map(rolling_min_long_series)
#     latest_data['liquid'] = latest_data['card_id'].map(liquid_series)

#     latest_data['rolling_min'] = latest_data[['rolling_min_short', 'rolling_min_long']].min(axis=1)
    
#     # Dynamic threshold (~2% above min, scaled with volatility)
#     latest_data['near_min_threshold'] = latest_data['rolling_min'] * (
#         1 + 0.02 * ((latest_data['rolling_max_short'] / latest_data['rolling_min'] + 1e-6) - 1)
#     )
    
#     is_dip = latest_data['sold_price'] <= latest_data['near_min_threshold']
#     is_rebound = latest_data['rebound_flag'] == True
#     is_liquid = latest_data['liquid'] >= 3
#     is_confirmed = latest_data['sold_price'] <= latest_data['average_price'] * 0.98
#     # 8. Combine criteria
#     meets_criteria = is_dip & is_rebound & is_liquid & is_confirmed
    
#     buy_signals = latest_data[meets_criteria].copy()

#     buy_signals.loc[:, 'buy_price'] = buy_signals['sold_price'] 

#     buy_signals['discount_%'] = (buy_signals['rolling_min_long'] - buy_signals['buy_price']) / buy_signals['rolling_min_long'] * 100

#     final_df = pd.DataFrame(buy_signals)
#     if final_df.empty:
#         return final_df
    
#     return final_df[['card_id', 'name', 'buy_price', 'discount_%']]



Z_SCORE_BUY_THRESHOLD = -1.3

def z_score_mean_reversion_strategy(market_prices_df, lookback_short=4, lookback_long=48):
    df_sorted = market_prices_df.sort_values('sale_time')
    df_sorted = df_sorted[
        (df_sorted['version'].str.strip().str.lower() != "gold rare") &
        (df_sorted['version'].notna())
    ].copy()

    latest_time = df_sorted['sale_time'].max()

    long_cutoff_time = latest_time - timedelta(hours=lookback_long)
    short_cutoff_time = latest_time - timedelta(hours=lookback_short)

    short_df = df_sorted[df_sorted['sale_time'] >= short_cutoff_time]
    long_df = df_sorted[df_sorted['sale_time'] >= long_cutoff_time]


    short_mean_series = short_df.groupby('card_id')['sold_price'].mean()
    long_mean_series = long_df.groupby('card_id')['sold_price'].mean()
    std_dev_series = long_df.groupby('card_id')['sold_price'].std()
    liquid_series = short_df.groupby('card_id')['sold_price'].count()

    valid_card_ids = short_mean_series.index.intersection(long_mean_series.index)
    latest_data = df_sorted.groupby('card_id').tail(1).copy()
    latest_data = latest_data[latest_data['card_id'].isin(valid_card_ids)].copy()

    # Historical Data

    rebound_series = short_df.groupby('card_id')['sold_price'].apply(
        lambda x: calculate_rebound_flag(x, window=5)
    )

    epsilon = 1e-9
    latest_data['short_mean'] = latest_data['card_id'].map(short_mean_series)
    latest_data['long_mean'] = latest_data['card_id'].map(long_mean_series)
    latest_data['std_dev'] = latest_data['card_id'].map(std_dev_series)
    latest_data['rebound_flag'] = latest_data['card_id'].map(rebound_series)
    latest_data['liquid'] = latest_data['card_id'].map(liquid_series)

    latest_data['z_score'] = (latest_data['short_mean'] - latest_data['long_mean']) / (latest_data['std_dev'] + epsilon)
    latest_data['z_score'] = latest_data['z_score'].fillna(999) 
    latest_data['discount_pct'] = ((latest_data['long_mean'] - latest_data['short_mean']) / latest_data['long_mean'] * 100)

    


    z_score_mask = latest_data['z_score'] <= Z_SCORE_BUY_THRESHOLD
    liquid_mask = latest_data['liquid'] >= 2

    final_buy_mask = z_score_mask & liquid_mask 

    # Select the final list of potential buys
    latest_data['score'] = latest_data['discount_pct'] / 100 - latest_data['z_score']
    final_candidates = latest_data[final_buy_mask].sort_values('score', ascending=False)

    final_candidates['buy_price'] = final_candidates['short_mean']
    signal_cutoff = latest_time - timedelta(hours=2)
    final_candidates = final_candidates[final_candidates['sale_time'] >= signal_cutoff]
    return final_candidates





# -------------------------------
# SHORT_HOURS = 4
# LONG_HOURS = 12
# SHORT_TRADES = 20
# LONG_TRADES = 60
# MIN_SHORT_SALES = 15
# MIN_LONG_SALES = 50

# def drop_strategy(market_prices_df):
#     df = market_prices_df.sort_values("sale_time")

#     cutoff_short = df['sale_time'].max() - pd.Timedelta(hours=SHORT_HOURS)
#     cutoff_long = df['sale_time'].max() - pd.Timedelta(hours=LONG_HOURS)

#     short_df = df[
#         (df['sale_time'] > cutoff_short) &
#         (df['sold_price'] > 0) 
#     ]

#     long_df = df[
#         (df['sale_time'] > cutoff_long) &
#         (df['sold_price'] > 0) 
#     ]

#     buy_candidates = []

#     for card_id, group in short_df.groupby('card_id'):
#         group = group.sort_values('sale_time', ascending=False)

#         if len(group) >= MIN_SHORT_SALES:
#             # Short-term avg
#             last_short_avg = group.head(SHORT_TRADES)['sold_price'].mean()

#             # Long-term avg
#             long_group = long_df[long_df['card_id'] == card_id].sort_values('sale_time', ascending=False)
#             if len(long_group) < MIN_LONG_SALES:
#                 continue

#             last_long_avg = long_group.head(LONG_TRADES)['sold_price'].mean()
#             sales_volume = len(long_group)

#             # Calculate drop %
#             drop_pct = (last_long_avg - last_short_avg) / last_long_avg * 100
#             if last_short_avg < 5000:  # skip unusable cards
#                 continue

#             # Suggested prices
#             buy_price = round(last_short_avg * 0.97)   # buy a bit below short avg
#             raw_sell_price = round(last_long_avg * 0.98)  # sell a bit below long avg

#             # Apply EA 5% tax
#             sell_price_after_tax = int(raw_sell_price * 0.95)

#             potential_profit = sell_price_after_tax - buy_price
#             profit_margin_pct = (potential_profit / buy_price) * 100

#             if profit_margin_pct < 3:
#                 continue

#             buy_candidates.append({
#                 "card_id": card_id,
#                 "name": group.iloc[0]["name"],
#                 "version": group.iloc[0]["version"],
#                 "last_short_avg": round(last_short_avg, 2),
#                 "last_long_avg": round(last_long_avg, 2),
#                 "discount_%": round(drop_pct, 2),
#                 "sales_volume": sales_volume,
#                 "buy_price": buy_price,
#                 "suggested_sell_raw": raw_sell_price,   # before tax
#                 "suggested_sell_after_tax": sell_price_after_tax,
#                 "potential_profit": potential_profit,
#                 "profit_margin_%": round(profit_margin_pct, 2),
#             })

#     final_df = pd.DataFrame(buy_candidates)
#     if final_df.empty:
#         return final_df
    
#     return final_df[['card_id', 'name', 'buy_price', 'discount_%']]

        


# ------------------- BOT RUNNER -------------------

if __name__ == "__main__":
    async def main():
        await bot.start(BOT_TOKEN)  # bot deletes messages in on_ready, then closes

    # Run Discord bot to clear messages first
    asyncio.run(main())
    engine = create_engine(
        DB_URL,
        pool_recycle=280,
        pool_pre_ping=True
        )

    # Then run market strategies after the bot is done
    with engine.connect() as conn:
        # strategy_discord_notify(conn, drop_strategy, 'drop')
        # strategy_discord_notify(conn, live_mean_reversion_strategy, 'live_mean')
        strategy_discord_notify(conn, z_score_mean_reversion_strategy, 'zscore')