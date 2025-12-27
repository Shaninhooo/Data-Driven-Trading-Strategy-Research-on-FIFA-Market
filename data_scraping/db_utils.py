import pymysql
from dotenv import load_dotenv
import os
import asyncio
from dateutil import parser
import pytz

load_dotenv()

def get_connection():
    return pymysql.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        database = os.getenv("DB_NAME"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

def initcardTable():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_platform_time ON market_sales(platform, sale_time);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_card_id ON market_sales(card_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_meta_card_id ON meta_cards(card_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cards_card_id ON cards(card_id);")


    # scraped_hrefs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta_cards (
            card_id INT PRIMARY KEY,
            href VARCHAR(255),
            FOREIGN KEY (card_id) REFERENCES cards(card_id)
        )
    """)

    # scraped_hrefs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hrefs (
            card_id INT PRIMARY KEY,
            href VARCHAR(255),
            version VARCHAR(20)
        )
    """)

    # cards table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            card_id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            game INT,
            version VARCHAR(20),
            nationality VARCHAR(50),
            league VARCHAR(50),
            club VARCHAR(50),
            position VARCHAR(3),
            rating INT,
            weak_foot INT,
            skill_move INT,
            height INT,
            accelerate VARCHAR(20),
            current_pc_price INT DEFAULT NULL,
            current_ps_price INT DEFAULT NULL
        )
    """)

    # card_playstyles
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_playstyles (
            card_id INT,
            playstyle VARCHAR(50) NOT NULL,
            plus TINYINT(1) NOT NULL DEFAULT 0,
            PRIMARY KEY(card_id, playstyle),
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # card_roles
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_roles (
            card_id INT,
            role VARCHAR(50) NOT NULL,
            position VARCHAR(50) NOT NULL,
            plus SMALLINT DEFAULT 1,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # card_pace_stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_pace_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            card_id INT,
            pace_overall INT,
            acceleration INT,
            sprint_speed INT,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # card_shooting_stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_shooting_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            card_id INT,
            shooting_overall INT,
            att_position INT,
            finishing INT,
            shot_power INT,
            long_shots INT,
            volleys INT,
            penalties INT,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # card_passing_stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_passing_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            card_id INT,
            passing_overall INT,
            vision INT,
            crossing INT,
            fk_acc INT,
            short_pass INT,
            long_pass INT,
            curve INT,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # card_dribbling_stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_dribbling_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            card_id INT,
            dribbling_overall INT,
            agility INT,
            balance INT,
            reactions INT,
            ball_control INT,
            dribbling INT,
            composure INT,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # card_defending_stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_defending_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            card_id INT,
            defending_overall INT,
            interceptions INT,
            heading_acc INT,
            def_aware INT,
            stand_tackle INT,
            slide_tackle INT,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # card_physical_stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_physical_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            card_id INT,
            physical_overall INT,
            jumping INT,
            stamina INT,
            strength INT,
            aggression INT,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)

    # market_sales
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_sales (
            sale_id INT AUTO_INCREMENT PRIMARY KEY,
            card_id INT,
            platform VARCHAR(20),
            sale_type VARCHAR(10),
            sale_time DATETIME NOT NULL,
            listed_price INT NOT NULL,
            sold_price INT,
            was_sold TINYINT(1) AS (sold_price IS NOT NULL AND sold_price <> 0) STORED,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
        )
    """)


    # recurring_events
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recurring_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_name TEXT NOT NULL,
            frequency TEXT NOT NULL,
            day_of_week INT,
            time_of_day TIME
        )
    """)

    # unique_events
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unique_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_name TEXT NOT NULL,
            version VARCHAR(20),
            start_datetime DATETIME NOT NULL,
            end_datetime DATETIME NOT NULL
        )
    """)

    print("Tables Initialized (MySQL)...")
    conn.commit()
    cur.close()
    conn.close()

def update_meta_cards():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Clear existing meta cards to refresh with the latest
            cur.execute("DELETE FROM meta_cards")

            # Insert all eligible cards as meta cards
            cur.execute("""
                INSERT INTO meta_cards (card_id, href)
                SELECT c.card_id, h.href
                FROM cards c
                JOIN hrefs h ON c.card_id = h.card_id
                WHERE c.current_pc_price > 25000
            """)

        conn.commit()
        print("✅ Meta cards updated successfully.")
    finally:
        conn.close()

def insert_batch_sales(sales_list):
    """
    Insert multiple players' sales into the DB in one query,
    skipping any older sales already in the DB.
    sales_list: list of dicts, each dict has card_id, platform, listed_price, sale_type, sale_time, sold_price
    """
    if not sales_list:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Build a dict of latest sale_time per card_id + platform
            card_platforms = {}
            card_ids = set(sale["card_id"] for sale in sales_list)

            # Fetch current max sale_time for all involved card_ids
            cur.execute(
                "SELECT card_id, platform, MAX(sale_time) AS max_time "
                "FROM market_sales WHERE card_id IN %s GROUP BY card_id, platform",
                (tuple(card_ids),)
            )

            adelaide = pytz.timezone("Australia/Adelaide")
            for row in cur.fetchall():
                t = row['max_time']
                if t:
                    if t.tzinfo is None:
                        t = adelaide.localize(t)
                    card_platforms[(row['card_id'], row['platform'].lower())] = t

            # Filter out old sales
            values = []
            for sale in sales_list:
                key = (sale['card_id'], sale['platform'].lower())
                sale_time = sale['sale_time']
                if isinstance(sale_time, str):
                    sale_time = parser.isoparse(sale_time)
                if sale_time.tzinfo is None:
                    sale_time = adelaide.localize(sale_time)

                if key in card_platforms and sale_time <= card_platforms[key]:
                    continue  # skip old sale

                values.append((
                    sale['card_id'],
                    sale['platform'],
                    sale['listed_price'],
                    sale['sale_type'],
                    sale_time,
                    sale['sold_price']
                ))

            if values:
                sql = """
                INSERT INTO market_sales
                (card_id, platform, listed_price, sale_type, sale_time, sold_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cur.executemany(sql, values)
                conn.commit()
    finally:
        conn.close()


async def async_insert_batch_sales(sales_list):
    await asyncio.to_thread(insert_batch_sales, sales_list)



def insert_sale_db(card_id, sale_data):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Get current max sale_time per platform
            cur.execute(
                "SELECT platform, MAX(sale_time) as max_time FROM market_sales WHERE card_id = %s GROUP BY platform",
                (card_id,)
            )

            adelaide = pytz.timezone("Australia/Adelaide")
            max_times = {}
            for row in cur.fetchall():
                if row['max_time']:
                    if row['max_time'].tzinfo is None:
                        max_times[row['platform'].lower()] = adelaide.localize(row['max_time'])
                    else:
                        max_times[row['platform'].lower()] = row['max_time']

            values = []
            for point in sale_data:
                platform = point['platform'].lower()
                sale_time = point['sale_time']

                if isinstance(sale_time, str):
                    sale_time = parser.isoparse(sale_time)

                if sale_time.tzinfo is None:
                    sale_time = adelaide.localize(sale_time)

                if platform in max_times and sale_time <= max_times[platform]:
                    continue

                values.append((
                    card_id,
                    platform,
                    point['listed_price'],
                    point['sale_type'],
                    sale_time,
                    point['sold_price']
                ))

            # --- Insert all new sales ---
            if values:
                sql = """
                INSERT INTO market_sales (card_id, platform, listed_price, sale_type, sale_time, sold_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cur.executemany(sql, values)

        conn.commit()

    finally:
        conn.close()



async def async_insert_sale_db(card_id, sale_data):
    await asyncio.to_thread(insert_sale_db, card_id, sale_data)


def insert_card(card_id, card_details, game_num):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cards (
                    card_id, name, game, version, nationality, league, club, position,
                    rating, weak_foot, skill_move, height, accelerate
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name),
                    game=VALUES(game),
                    version=VALUES(version),
                    nationality=VALUES(nationality),
                    league=VALUES(league),
                    club=VALUES(club),
                    position=VALUES(position),
                    rating=VALUES(rating),
                    weak_foot=VALUES(weak_foot),
                    skill_move=VALUES(skill_move),
                    height=VALUES(height),
                    accelerate=VALUES(accelerate);
            """, (
                card_id,
                card_details.get("name"),
                game_num,
                card_details.get("version"),
                card_details.get("nation"),
                card_details.get("league"),
                card_details.get("club"),
                card_details.get("position"),
                card_details.get("rating"),
                card_details.get("weakfoot"),
                card_details.get("skills"),
                card_details.get("height"),
                card_details.get("accelerate")
            ))
        conn.commit()
    finally:
        conn.close()

def insert_card_playstyles(card_id, playstyles_list):
    if not playstyles_list:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Ensure card exists in `cards` (avoid FK constraint errors)
            cur.execute("SELECT 1 FROM cards WHERE card_id = %s LIMIT 1", (card_id,))
            if not cur.fetchone():
                print(f"⚠️ Skipping playstyles: card {card_id} not found in `cards`.")
                return

            for ps in playstyles_list:
                cur.execute("""
                    INSERT INTO card_playstyles (card_id, playstyle, plus)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE plus = VALUES(plus)
                """, (
                    card_id,
                    ps.get("playstyle"),
                    ps.get("plus")
                ))
        conn.commit()
    finally:
        conn.close()



def insert_card_roles(card_id, roles):
    if not roles:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM cards WHERE card_id = %s LIMIT 1", (card_id,))
            if not cur.fetchone():
                print(f"⚠️ Skipping roles: card {card_id} not found in `cards`.")
                return

            for r in roles:
                cur.execute("""
                    INSERT INTO card_roles (card_id, position, role, plus)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE plus = VALUES(plus)
                """, (
                    card_id,
                    r.get("position"),
                    r.get("role"),
                    r.get("plus")
                ))
        conn.commit()
    finally:
        conn.close()



def insert_card_stats(card_id, stats_list):
    if not stats_list:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM cards WHERE card_id = %s LIMIT 1", (card_id,))
            if not cur.fetchone():
                print(f"⚠️ Skipping stats: card {card_id} not found in `cards`.")
                return

            stats_table_mapping = {
                "card_pace_stats": "pace",
                "card_shooting_stats": "shooting",
                "card_passing_stats": "passing",
                "card_dribbling_stats": "dribbling",
                "card_defending_stats": "defending",
                "card_physical_stats": "physical"
            }

            for table, category in stats_table_mapping.items():
                substats = stats_list.get(category, {})
                if not substats:
                    continue

                columns = list(substats.keys())
                values = list(substats.values())

                all_columns = ["card_id"] + columns
                placeholders = ", ".join(["%s"] * len(all_columns))
                update_clause = ", ".join([f"{col}=VALUES({col})" for col in columns])

                sql = f"""
                    INSERT INTO {table} ({', '.join(all_columns)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """
                cur.execute(sql, [card_id] + values)

        conn.commit()
    finally:
        conn.close()




def drop_all_tables():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("DROP SCHEMA public CASCADE;")
    cur.execute("CREATE SCHEMA public;")
    
    conn.commit()
    cur.close()
    conn.close()
    print("All tables dropped.")

# drop_all_tables()
# initcardTable()