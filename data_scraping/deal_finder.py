import pandas as pd
import numpy as np
from discordwebhook import Discord
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import discord
from discord.ext import commands
import asyncio
from datetime import datetime, timedelta
import logging
from sklearn.linear_model import LinearRegression
import numpy as np


# Set logging level for better visibility
logging.basicConfig(level=logging.INFO)

load_dotenv()

# --- Discord Setup ---
intents = discord.Intents.default()
intents.message_content = True 
bot = commands.Bot(command_prefix="!", intents=intents)

BOT_TOKEN = os.getenv("BOT_TOKEN")

DISCORD_WEBHOOKS = {
    "DISCORD_CHANNEL_ICON": os.getenv("ICON_WEBHOOK"),
    "DISCORD_CHANNEL_GOLD": os.getenv("GOLD_WEBHOOK"),
    "DISCORD_CHANNEL_PROMO": os.getenv("PROMO_WEBHOOK"),
    "DISCORD_CHANNEL_HERO": os.getenv("HERO_WEBHOOK"),
    "DISCORD_CHANNEL_BEST": os.getenv("BEST_WEBHOOK")
}
DB_URL = os.getenv("DB_URL") 

@bot.event
async def on_ready():
    """Purges all target channels (except pinned messages) before analysis starts."""
    print(f'✅ Logged in as {bot.user}')

    # List of channel IDs you want to clean up
    CHANNEL_IDS = [
        1421417739213471795,  # ICON deals channel
        1432222288631037953,  # GOLD deals channel
        1434118996420202508,  # PROMO deals channel
        1434754189887537252,   # HERO deals channel
        1436295698554552422   # BEST deals channel
    ]

    for channel_id in CHANNEL_IDS:
        channel = bot.get_channel(channel_id)
        if not channel:
            print(f"⚠️ Channel ID {channel_id} not found (bot may lack access).")
            continue

        try:
            deleted = await channel.purge(limit=1000, check=lambda m: not m.pinned)
            print(f"🧹 {channel.name}: Deleted {len(deleted)} messages (pinned preserved)")
        except Exception as e:
            print(f"⚠️ Failed to purge {channel.name}: {e}")

    print("✅ All channels cleaned up. Closing bot...")
    await bot.close()

def send_discord_message(message: str, channel=None):
    """Sends a message using the appropriate Discord webhook."""
    webhook_url = DISCORD_WEBHOOKS.get(channel)

    if not webhook_url:
        print(f"⚠️ No Discord webhook configured for channel: {channel}")
        return

    try:
        discord_webhook_client = Discord(url=webhook_url)
        discord_webhook_client.post(content=message)
        print(f"✅ Sent message to {channel}")
    except Exception as e:
        print(f"⚠️ Error sending webhook message to {channel}: {e}")

# ------------------- DATA FETCHING -------------------

def fetch_meta_sales(conn, platform="pc", hours=172):
    """Fetches sales data for meta cards over a specified period."""
    cutoff_time = datetime.now() - timedelta(hours=hours)
    
    query = f"""
        SELECT 
            ms.card_id,
            c.name,
            c.version,
            c.current_ps_price,
            c.current_pc_price,
            ms.sale_time,
            ms.sold_price,
            ms.platform,
            ms.was_sold,
            ms.listed_price
        FROM market_sales ms
        JOIN cards c ON ms.card_id = c.card_id
        JOIN meta_cards m ON ms.card_id = m.card_id
        WHERE ms.platform = '{platform}'
        AND ms.sale_time >= '{cutoff_time}'
    """
    
    return pd.read_sql(query, conn)

def clean_group(g):
    """Removes outliers (top/bottom 5%) and low prices from a card's sales history."""
    g = g[g['sold_price'].notnull()]  # keep zero prices, just remove NaN
    if len(g) < 10:
        return g
    # Remove top and bottom 5% as outliers
    low = g['sold_price'].quantile(0.05)
    high = g['sold_price'].quantile(0.95)
    return g[(g['sold_price'] >= low) & (g['sold_price'] <= high)]

def strategy_discord_notify(conn, strategy):
    """
    Runs a trading strategy for all platforms and categories (Icon, Gold, Promo),
    formats messages, and sends Discord notifications for top opportunities.
    Each category sends messages to its own Discord channel.
    """
    platforms = ["pc"]

    # Map each category to its respective Discord channel or webhook
    CHANNELS = {
        "ICON": "DISCORD_CHANNEL_ICON",
        "GOLD": "DISCORD_CHANNEL_GOLD",
        "PROMO": "DISCORD_CHANNEL_PROMO",
        "HERO": "DISCORD_CHANNEL_HERO",
        "BEST": "DISCORD_CHANNEL_BEST"  # Using PROMO channel for HERO as well
    }

    for plat in platforms:
        df = fetch_meta_sales(conn, platform=plat)
        print(f"Data received for {plat} ({len(df)} rows)")

        if df.empty:
            print(f"No data on {plat}")
            continue

        # Clean data by card group
        df_clean = df.groupby("card_id", group_keys=False)\
                     .apply(clean_group)\
                     .reset_index(drop=True)

        # General market health message (to a general channel)
        # send_discord_message(f"--- 🌐 **{plat.upper()}** Market Analysis ---")

        # market_status = check_for_live_crash_signal(df_clean)
        # send_discord_message(market_status["report"], channel="DISCORD_CHANNEL_GOLD")

        # Category filtering
        icon_df = df_clean[df_clean["version"].str.contains("icon", case=False, na=False)]
        gold_df = df_clean[df_clean["version"].str.contains("gold", case=False, na=False)]
        promo_df = df_clean[
            ~df_clean["version"].str.contains("icon|gold|hero", case=False, na=False)
        ]
        hero_df = df_clean[df_clean["version"].str.contains("hero", case=False, na=False)]

        categories = {
            "ICON": icon_df,
            "GOLD": gold_df,
            "PROMO": promo_df,
            "HERO": hero_df,
            "BEST": df_clean  # Overall best deals across all categories
        }

        for category, df_cat in categories.items():
            channel = CHANNELS.get(category)
            print(f"\n[{plat.upper()} - {category}] → {len(df_cat)} cards")

            if df_cat.empty:
                send_discord_message(
                    f"⚪ **{plat.upper()} {category}**: No cards available.",
                    channel=channel
                )
                continue

            candidates = strategy(df_cat, plat)

            if candidates.empty:
                send_discord_message(
                    f"✅ **{plat.upper()} {category}**: No dip-buy signals found.",
                    channel=channel
                )
                continue

            candidates = candidates.sort_values("confidence", ascending=False)
            top_n = candidates.head(5)

            send_discord_message(
                f"🚨 **{plat.upper()} {category}** Top Dip Buy Signals ({len(top_n)} found):",
                channel=channel
            )

            for _, row in top_n.iterrows():
                name = row.get("name", "Unknown")
                version = row.get("version", "")
                buy_price = row.get(f"current_{plat}_price", 0)
                confidence = row.get("confidence", 0)
                drop_pct = row.get("drop_pct", 0)
                long_mean = row.get("long_mean", None)
                price_status = row.get("price_status", "Unknown")
                demand_status = row.get("demand_status", "Unknown")

                msg = (
                    f"🎯 **{category} Deal Alert**\n"
                    f"🎴 Card: **{name}** ({version})\n"
                    f"💰 Buy Price: **{int(buy_price):,}**\n"
                    f"📉 Drop %: `{drop_pct:.2%}`\n"
                    f"🔮 Confidence Score: `{confidence:.3f}`\n"
                )

                if long_mean:
                    msg += f"📊 24h Mean: `{int(long_mean):,}`\n"
                if price_status:
                    msg += f"📈 Price Status: `{price_status}`\n"
                if demand_status:
                    msg += f"🔥 Demand Status: `{demand_status}`\n"

                print(f"Sending Discord alert for {name} ({plat}-{category}) → {confidence:.3f}")
                send_discord_message(msg, channel=channel)

            print(f"✅ Sent {len(top_n)} messages for {plat}-{category}")



# ------------------- STRATEGIES (Fixed History Check) -------------------


def compute_price_slope(df):
    """
    Compute linear regression slope of price vs time (in hours).
    Negative slope = price dropping.
    """
    if len(df) < 3:
        return np.nan

    t = (df["sale_time"] - df["sale_time"].min()).dt.total_seconds().to_numpy().reshape(-1, 1)
    p = df["sold_price"].to_numpy().reshape(-1, 1)

    model = LinearRegression()
    model.fit(t, p)

    # slope = price change per second → convert to per hour
    return float(model.coef_[0][0] * 3600)


def Deep_Dip_Buy_Volume_Confirm(
    market_prices_df,
    platform="pc",
    short_window=4,
    long_window=24,
    dip_threshold=0.09,
    min_history_hours=96,
    confirm_window=1
):
    """
    Detect buy signals for cards experiencing a controlled crash:
      - price dropped significantly
      - near recent low
      - volume not exploding (panic)
      - decent liquidity
      - stable within last X hours
    """
    if market_prices_df.empty or len(market_prices_df) < 5:
        return pd.DataFrame()

    df_sorted = market_prices_df.sort_values("sale_time")
    latest_time = df_sorted["sale_time"].max()

    num_unsold = (market_prices_df["was_sold"] == 0).sum()
    print("Total unsold listings:", num_unsold)

    # Time cutoffs
    long_cutoff = latest_time - timedelta(hours=long_window)
    short_cutoff = latest_time - timedelta(hours=short_window)
    confirm_cutoff = latest_time - timedelta(hours=confirm_window)
    min_history = timedelta(hours=min_history_hours)

    sold_df = df_sorted[df_sorted["was_sold"] == 1]

    long_df = sold_df[sold_df["sale_time"] >= long_cutoff]
    short_df = sold_df[sold_df["sale_time"] >= short_cutoff]
    confirm_df = sold_df[sold_df["sale_time"] >= confirm_cutoff]

    # ------------------------------
    # Aggregate statistics
    # ------------------------------
    long_mean = long_df.groupby("card_id")["sold_price"].mean()
    short_low = short_df.groupby("card_id")["sold_price"].min()
    long_volume = long_df.groupby("card_id")["sold_price"].count()
    short_volume = short_df.groupby("card_id")["sold_price"].count()
    first_sale_time = df_sorted.groupby("card_id")["sale_time"].min()
    confirm_std = confirm_df.groupby("card_id")["sold_price"].std()

    latest_data = df_sorted.groupby("card_id").tail(1).copy()

    # Map metrics to latest rows
    latest_data["long_mean"] = latest_data["card_id"].map(long_mean)
    latest_data["short_low"] = latest_data["card_id"].map(short_low)
    latest_data["short_volume"] = latest_data["card_id"].map(short_volume)
    latest_data["first_sale_time"] = latest_data["card_id"].map(first_sale_time)

    latest_data["long_avg_hourly_volume"] = latest_data["card_id"].map(long_volume) / long_window
    latest_data["short_avg_hourly_volume"] = latest_data["card_id"].map(short_volume) / short_window
    latest_data["confirm_std"] = latest_data["card_id"].map(confirm_std)

    # Fix invalids
    latest_data["confirm_std"] = latest_data["confirm_std"].fillna(latest_data["long_mean"] * 0.03)
    latest_data["long_mean"].replace(0, np.nan, inplace=True)
    latest_data["short_avg_hourly_volume"].fillna(0, inplace=True)
    latest_data["long_avg_hourly_volume"].replace(0, np.nan, inplace=True)

    latest_data.dropna(subset=["long_mean"], inplace=True)

    # -----------------------------------------
    # Price-based indicators
    # -----------------------------------------
    current_price_col = f"current_{platform}_price"

    latest_data["drop_pct"] = (
        (latest_data["long_mean"] - latest_data[current_price_col])
        / latest_data["long_mean"]
    )

    latest_data["near_low_mask"] = (
        latest_data[current_price_col] <= 1.05 * latest_data["short_low"]
    )

    # -----------------------------------------
    # Volume stability logic
    # -----------------------------------------
    high_price = latest_data[current_price_col] > 200000

    latest_data["volume_stable_mask"] = np.where(
        high_price,
        latest_data["short_avg_hourly_volume"] <= 2.0 * latest_data["long_avg_hourly_volume"],
        latest_data["short_avg_hourly_volume"] <= 1.25 * latest_data["long_avg_hourly_volume"]
    )

    latest_data["liquid_mask"] = np.where(
        high_price,
        latest_data["short_volume"] >= 2,
        latest_data["short_volume"] >= 5
    )

    latest_data["history_mask"] = (
        latest_time - latest_data["first_sale_time"] >= min_history
    )

    # -----------------------------------------
    # Sell through rate
    # -----------------------------------------
    long_df_all = df_sorted[df_sorted["sale_time"] >= long_cutoff]
    floor_min = long_df_all.groupby("card_id")["listed_price"].min()

    floor_cutoff = 1.30 * floor_min
    long_df_all = long_df_all.merge(floor_cutoff.rename("floor_cutoff"), on="card_id")

    floor_df = long_df_all[long_df_all["listed_price"] <= long_df_all["floor_cutoff"]]

    sold_count = floor_df[floor_df["was_sold"] == 1].groupby("card_id").size()
    expired_count = floor_df[floor_df["was_sold"] == 0].groupby("card_id").size()

    sell_through = sold_count / (sold_count + expired_count)
    sell_through = sell_through.fillna(0)

    latest_data["sell_through_rate"] = latest_data["card_id"].map(sell_through).fillna(0)

    # -----------------------------------------
    # Confirm no-new-lows check
    # -----------------------------------------
    confirm_min = confirm_df.groupby("card_id")["sold_price"].min()
    latest_data["confirm_mask"] = (
        latest_data[current_price_col] >= latest_data["card_id"].map(confirm_min)
    )

    # -----------------------------------------
    # Price slope
    # -----------------------------------------
    price_slope = short_df.groupby("card_id").apply(compute_price_slope)
    latest_data["price_slope"] = latest_data["card_id"].map(price_slope)

    latest_data["slope_pct_per_hr"] = latest_data["price_slope"] / latest_data["long_mean"]

    # -----------------------------------------
    # Buy mask
    # -----------------------------------------
    buy_mask = (
        (latest_data["drop_pct"] >= dip_threshold)
        & latest_data["near_low_mask"]
        & (latest_data["volume_stable_mask"] | latest_data["confirm_mask"])
        & latest_data["liquid_mask"]
        & latest_data["history_mask"]
    )

    # -----------------------------------------
    # Scoring
    # -----------------------------------------
    latest_data["stability_score"] = 1 / (1 + (latest_data["confirm_std"] / latest_data["long_mean"]).fillna(1))
    volume_ratio = (latest_data["short_avg_hourly_volume"] / latest_data["long_avg_hourly_volume"]).clip(0, 1).fillna(0)

    latest_data["confidence"] = (
        0.6 * latest_data["drop_pct"].fillna(0)
        + 0.2 * latest_data["stability_score"].fillna(0)
        + 0.1 * volume_ratio
        + 0.1 * latest_data["sell_through_rate"]
    )

    latest_data["demand_status"] = pd.cut(
        latest_data["sell_through_rate"],
        bins=[0, 0.2, 0.4, 0.7, 1],
        labels=["Dead", "Weak", "Healthy", "Hot"]
    )

    latest_data["price_status"] = pd.cut(
        latest_data["slope_pct_per_hr"],
        bins=[-np.inf, -0.02, -0.01, 0.0, 0.01, np.inf],
        labels=["Crashing", "Deep Dip", "Dipping", "Stable", "Rising"]
    )

    # -----------------------------------------
    # Final selection
    # -----------------------------------------
    signals = latest_data[buy_mask].copy()
    signals["buy_price"] = signals[current_price_col]

    return signals.sort_values("confidence", ascending=False)





# ------------------- MARKET HEALTH ----------------

# --- GLOBAL PARAMETERS ---
LIVE_INTERVAL_MINUTES = 30
CRASH_ZSCORE_THRESHOLD = -2.0 
ROLLING_WINDOW_BINS = 20 
MIN_VOLUME_THRESHOLD_FALLBACK = 800
MIN_PARTICIPATION_THRESHOLD_FALLBACK = 150
HISTORICAL_DAYS = 7


# --- PREPROCESSING ---
def preprocess_sales_data(sales_df):
    """Prepares sales data and extracts the last two complete time bins."""
    if sales_df.empty:
        return None, None, None, None

    end_time = sales_df['sale_time'].max()
    start_time = end_time - timedelta(days=HISTORICAL_DAYS)
    sales_df = sales_df[(sales_df['sale_time'] >= start_time)].copy()

    sales_df = sales_df.sort_values('sale_time').copy()
    sales_df['time_bin'] = sales_df['sale_time'].dt.floor(f'{LIVE_INTERVAL_MINUTES}min')

    all_bins = sales_df['time_bin'].unique()
    
    if len(all_bins) < 3:
        return sales_df, None, None, None
        
    # [-3] is the previous full bin, [-2] is the current latest full bin ([-1] is typically incomplete)
    previous_full_bin, current_full_bin = all_bins[-3], all_bins[-2]
    
    return sales_df, previous_full_bin, current_full_bin, all_bins

def calculate_market_index_and_velocity(sales_df):
    """
    Computes market index (volume-weighted average), velocity (log returns), 
    and Z-score for the latest completed time bin.
    """
    sales_df_processed, prev_bin, curr_bin, all_bins = preprocess_sales_data(sales_df)
    
    if sales_df_processed is None or curr_bin is None or len(all_bins) < 2: 
        return None, None, None, None 

    def _calculate_robust_weighted_average(x):
        """Calculates a robust weighted average, weighting by price itself."""
        prices = x['sold_price']
        weights = x['sold_price'] 
        
        if weights.sum() == 0:
            return np.nan 
        
        return np.average(prices, weights=weights)

    # 1. Calculate Volume-Weighted Market Index for ALL historical bins (7 days)
    full_market_index = (sales_df_processed.groupby('time_bin')
                         .apply(_calculate_robust_weighted_average)
                         .dropna()) # Drop bins with no valid trades

    # 2. Compute log returns (velocity) for ALL valid bins
    velocities = np.log(full_market_index / full_market_index.shift(1)) * 100
    velocities = velocities.dropna()
    
    if len(velocities) < 2:
        return None, None, None, None

    # Use the second to last index/velocity as the "latest" full one
    latest_velocity = velocities.iloc[-1]
    latest_index = full_market_index.iloc[-1]
    
    # Check for enough velocity data for stable Z-score calculations
    if len(velocities) < ROLLING_WINDOW_BINS:
         # Use the latest velocity but z_velocity is 0 (unreliable)
         z_velocity = 0
         smoothed_velocity = latest_velocity
    else:
        # 3. Smooth velocity using rolling window
        smoothed_velocity = velocities.rolling(window=ROLLING_WINDOW_BINS, min_periods=2).mean().iloc[-1]
        
        # 4. Z-score normalization for adaptive detection (using ALL historical velocities)
        z_velocity = (latest_velocity - velocities.mean()) / velocities.std()

    return latest_index, smoothed_velocity, z_velocity, curr_bin


def calculate_volume_and_participation(sales_df):
    """Returns adaptive thresholds and latest volume/participation metrics."""
    sales_df_processed, prev_bin, curr_bin, all_bins = preprocess_sales_data(sales_df)
    if sales_df_processed is None or curr_bin is None:
        return 0, 0, MIN_VOLUME_THRESHOLD_FALLBACK, MIN_PARTICIPATION_THRESHOLD_FALLBACK

    # Adaptive baselines from full 7-day history
    avg_volume = sales_df_processed.groupby('time_bin').size().mean()
    avg_participation = sales_df_processed.groupby('time_bin')['card_id'].nunique().mean()

    # Thresholds are 60% of the historical average for the given interval
    min_volume = int(avg_volume * 0.6)
    min_participation = int(avg_participation * 0.6)

    # Metrics for latest bin
    latest_data = sales_df_processed[sales_df_processed['time_bin'] == curr_bin]
    total_volume = latest_data.shape[0]
    unique_cards = latest_data['card_id'].nunique()

    return total_volume, unique_cards, min_volume, min_participation


# --- MAIN CRASH DETECTION ---
def check_for_live_crash_signal(sales_df):
    """Evaluates the latest 30-min market window for crash conditions."""
    latest_index, smoothed_velocity, z_velocity, latest_bin = calculate_market_index_and_velocity(sales_df)
    
    if latest_index is None or latest_bin is None:
        return {"status": "NO_DATA", "report": "Not enough data to detect crash (requires two full 30-min bins)."}

    total_volume, unique_cards, min_vol, min_part = calculate_volume_and_participation(sales_df)

    # Use Z-score for crash condition check
    is_price_crashing = (z_velocity < CRASH_ZSCORE_THRESHOLD)
    
    # Use Adaptive thresholds for validation
    is_volume_valid = (total_volume > min_vol)
    is_participation_valid = (unique_cards > min_part)

    latest_time_str = latest_bin.strftime('%H:%M:%S UTC')

    # --- REPORT LOGIC ---
    report = [
        f"--- Live Market Check (Time: {latest_time_str}) ---",
        f"History Used: {HISTORICAL_DAYS} Days | Interval: {LIVE_INTERVAL_MINUTES} min",
        f"Threshold: Z < {CRASH_ZSCORE_THRESHOLD}",
        f"Velocity (smoothed): {smoothed_velocity:.2f}% | **Z-Score: {z_velocity:.2f}**",
        f"Volume: {total_volume:,.0f} (min {min_vol:,.0f}) | Unique Cards: {unique_cards:,.0f} (min {min_part:,.0f})"
    ]

    # --- SIGNAL CLASSIFICATION ---
    if is_price_crashing and is_volume_valid and is_participation_valid:
        report.append("🚨 **VALIDATED CRASH SIGNAL** — High volume & widespread panic. **PANIC FLUSH**")
        status = "CRASH_CONFIRMED"

    elif is_price_crashing and (is_volume_valid or is_participation_valid):
        report.append("⚠️ **FOCUSED CRASH/VOLATILITY** — Drop confirmed by *some* activity, but not both metrics.")
        status = "CRASH_LOCALIZED"

    elif is_price_crashing and not is_volume_valid and not is_participation_valid:
        report.append("📉 **LOW-LIQUIDITY VOLATILITY** — Sharp move, but volume is too weak. **ILLIQUID FALL** (Avoid!)")
        status = "VOLATILITY_ONLY"

    else:
        report.append("✅ **Market Stable or Recovering.**")
        status = "STABLE"

    return {
        "status": status,
        "velocity": smoothed_velocity,
        "z_velocity": z_velocity,
        "volume": total_volume,
        "unique_cards": unique_cards,
        "report": "\n".join(report)
    }


# ------------------- BOT RUNNER -------------------


async def run_bot_cleanup():
    """Runs the bot to execute the cleanup event, then closes."""
    try:
        await bot.start(BOT_TOKEN)
    except discord.errors.LoginFailure:
        print("❌ Discord Login Failure. Check BOT_TOKEN.")
    except Exception as e:
        print(f"❌ An error occurred during bot cleanup: {e}")
    finally:
        await bot.close()  # ensures aiohttp session closes cleanly


async def run_deal_notifications():
    """The main execution flow: cleanup, then database connection and strategy run."""
    
    # 1. Run bot to delete messages (this runs asynchronously and then closes)
    await run_bot_cleanup()

    # 2. Run strategies after bot cleanup
    if not DB_URL:
        print("❌ DB_URL environment variable is not set. Cannot connect to database.")
        return

    try:
        # Improved DB engine configuration for connection pooling
        engine = create_engine(
            DB_URL, 
            pool_recycle=3600, # Reconnect once per hour
            pool_pre_ping=True, 
            pool_size=5, 
            max_overflow=10
        )
        with engine.connect() as conn:
            print("\nSuccessfully connected to database. Running strategy...")
            strategy_discord_notify(conn, Deep_Dip_Buy_Volume_Confirm)
        engine.dispose()
        print("\n--- Script run complete. ---")
    

    except Exception as e:
        send_discord_message(f"🚨 **TRADER ERROR** 🚨\nDatabase or execution failed: {type(e).__name__}: {str(e)}")
        print(f"❌ Database connection or strategy execution failed: {e}")

        

