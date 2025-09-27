# import pandas as pd
# from db_utils import get_connection
from discordwebhook import Discord
import os

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
discord = Discord(url=DISCORD_WEBHOOK)

def send_discord_message(message: str):
    """Send a message to Discord via webhook"""
    if not DISCORD_WEBHOOK:
        print("⚠️ No Discord webhook set in environment variables.")
        return
    discord.post(content=message)


def fetch_drop_candidates(conn, platform="pc"):
    """Fetch Gold Rare drop candidates directly from SQL"""
    query = f"""
    SELECT 
        ms.card_id,
        c.name,
        AVG(CASE WHEN ms.sale_time >= NOW() - INTERVAL 1 HOUR THEN ms.sold_price END) AS short_avg,
        AVG(ms.sold_price) AS long_avg,
        COUNT(*) AS sales_volume
    FROM market_sales ms
    JOIN cards c ON ms.card_id = c.card_id
    WHERE ms.sold_price > 0
      AND ms.platform = '{platform}'
      AND c.version = 'Gold Rare'
      AND ms.sale_time >= NOW() - INTERVAL 12 HOUR
    GROUP BY ms.card_id, c.name
    HAVING sales_volume >= 20
    """
    return pd.read_sql(query, conn)


def fetch_icon_fluctuations(conn, platform="pc"):
    """Fetch Icon fluctuation candidates directly from SQL"""
    query = f"""
    SELECT 
        ms.card_id,
        c.name,
        MIN(ms.sold_price) AS min_price,
        MAX(ms.sold_price) AS max_price,
        AVG(ms.sold_price) AS avg_price,
        COUNT(*) AS sales_volume
    FROM market_sales ms
    JOIN cards c ON ms.card_id = c.card_id
    WHERE ms.sold_price > 0
      AND ms.platform = '{platform}'
      AND c.version = 'All Icons'
      AND ms.sale_time >= NOW() - INTERVAL 12 HOUR
    GROUP BY ms.card_id, c.name
    HAVING (MAX(ms.sold_price) - MIN(ms.sold_price)) / AVG(ms.sold_price) * 100 >= 5
       AND sales_volume >= 5
    """
    return pd.read_sql(query, conn)


def drop_strategy(conn):
    platforms = ["pc", "ps"]

    for plat in platforms:
        df = fetch_drop_candidates(conn, platform=plat)

        if df.empty:
            print(f"No Gold Rare drops found on {plat}")
            continue

        buy_candidates = []

        for _, row in df.iterrows():
            last_short_avg = row['short_avg']
            last_long_avg = row['long_avg']
            sales_volume = row['sales_volume']

            # Drop detection
            if last_short_avg < 0.90 * last_long_avg and last_short_avg >= 5000:
                buy_price = round(last_short_avg * 0.97)
                sell_price = round(last_long_avg * 0.98)
                potential_profit = sell_price - buy_price
                drop_percent = (last_long_avg - last_short_avg) / last_long_avg * 100

                # Investment rating
                if drop_percent >= 15 and (potential_profit / buy_price) * 100 >= 5 and sales_volume >= 30:
                    rating = "🔥 High"
                elif drop_percent >= 10 and (potential_profit / buy_price) * 100 >= 3:
                    rating = "⚡ Medium"
                else:
                    rating = "⚠️ Low"

                buy_candidates.append({
                    "name": row['name'],
                    "last_short_avg": round(last_short_avg, 2),
                    "last_long_avg": round(last_long_avg, 2),
                    "drop_%": round(drop_percent, 2),
                    "sales_volume": sales_volume,
                    "suggested_buy": buy_price,
                    "suggested_sell": sell_price,
                    "potential_profit": potential_profit,
                    "investment_rating": rating
                })

        buy_df = pd.DataFrame(buy_candidates)
        if not buy_df.empty:
            buy_df = buy_df.sort_values(["investment_rating", "drop_%"], ascending=[False, False])
            print(f"🔥 Gold Rare Drop Candidates on {plat} 🔥")
            print(buy_df.head(10))

            # Send top 5 deals to Discord
            for _, row in buy_df.head(5).iterrows():
                msg = (
                    f"📊 **{plat.upper()} Deal Alert!**\n"
                    f"Card: {row['name']}\n"
                    f"Drop: {row['drop_%']}%\n"
                    f"Buy @ {row['suggested_buy']} | Sell @ {row['suggested_sell']}\n"
                    f"Profit: {row['potential_profit']} | Rating: {row['investment_rating']}"
                )
                send_discord_message(msg)


def icon_fluctuation_strategy(conn):
    platforms = ["pc", "ps"]

    for plat in platforms:
        df = fetch_icon_fluctuations(conn, platform=plat)

        if df.empty:
            print(f"No Icon fluctuations found on {plat}")
            continue

        fluctuation_candidates = []

        for _, row in df.iterrows():
            min_price = row['min_price']
            avg_price = row['avg_price']
            max_price = row['max_price']
            sales_volume = row['sales_volume']

            buy_price = round(min_price * 1.02)
            sell_price = round(avg_price * 0.98)
            profit_margin = round((sell_price - buy_price) / buy_price * 100, 2)

            if profit_margin > 5:
                fluctuation_candidates.append({
                    "name": row['name'],
                    "avg_price": int(avg_price),
                    "min_price": int(min_price),
                    "max_price": int(max_price),
                    "spread_%": round((max_price - min_price) / avg_price * 100, 2),
                    "sales_volume": sales_volume,
                    "best_buy": buy_price,
                    "best_sell": sell_price,
                    "profit_margin_%": profit_margin
                })

        fluctuation_df = pd.DataFrame(fluctuation_candidates)
        if not fluctuation_df.empty:
            fluctuation_df = fluctuation_df.sort_values("profit_margin_%", ascending=False)
            print(f"💎 Icon Fluctuation Candidates on {plat}")
            print(fluctuation_df.head(10))

            # Send top 5 deals to Discord
            for _, row in fluctuation_df.head(5).iterrows():
                msg = (
                    f"💎 Icon Fluctuation: {row['name']}\n"
                    f"🟢 Buy ~ {row['best_buy']:,}\n"
                    f"🔴 Sell ~ {row['best_sell']:,}\n"
                    f"📈 Margin: {row['profit_margin_%']}%"
                )
                send_discord_message(msg)


if __name__ == "__main__":
    send_discord_message("Hello Gooners!")