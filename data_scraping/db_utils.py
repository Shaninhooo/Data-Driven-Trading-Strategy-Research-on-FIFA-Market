import psycopg
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import re

load_dotenv()

def get_connection():
    return psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )


def initcardTable():
    conn = get_connection()
    cur = conn.cursor()
    # Create card table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            card_id SERIAL PRIMARY KEY,
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
            accelerate VARCHAR(20)
        )
    """)

    # Create card Playstyle Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_playstyles (
            card_id INT REFERENCES cards(id),
            playstyle VARCHAR(50) NOT NULL,
            plus BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY(card_id, playstyle)
        )
    """)

    # Create card Roles Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_roles (
            card_id INT REFERENCES cards(id),
            role VARCHAR(50) NOT NULL,
            position VARCHAR(50) NOT NULL, 
            plus SMALLINT DEFAULT 1
        )
    """)
    
    # Create Pace Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_pace_stats (
            id SERIAL PRIMARY KEY,
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            pace_overall INT,
            acceleration INT,
            sprint_speed INT
        )
    """)

    # Create Shooting Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_shooting_stats (
            id SERIAL PRIMARY KEY,
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            shooting_overall INT,
            att_position INT,
            finishing INT,
            shot_power INT,
            long_shots INT,
            volleys INT,
            penalties INT
        )
    """)

    # Create Passing Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_passing_stats (
            id SERIAL PRIMARY KEY,
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            passing_overall INT,
            vision INT,
            crossing INT,
            fk_acc INT,
            short_pass INT,
            long_pass INT,
            curve INT
        )
    """)

    # Create Dribbling Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_dribbling_stats (
            id SERIAL PRIMARY KEY,
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            dribbling_overall INT,
            agility INT,
            balance INT,
            reactions INT,
            ball_control INT,
            dribbling INT,
            composure INT
        )
    """)

    # Create Defending Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_defending_stats (
            id SERIAL PRIMARY KEY,
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            defending_overall INT,
            interceptions INT,
            heading_acc INT,
            def_aware INT,
            stand_tackle INT,
            slide_tackle INT
        )
    """)

    # Create Physical Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_physical_stats (
            id SERIAL PRIMARY KEY,
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            physical_overall INT,
            jumping INT,
            stamina INT,
            strength INT,
            aggression INT
        )
    """)

    # Create Value History
    cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            pc_value INT,
            console_value INT,
            date_time  TIMESTAMP NOT NULL
        )
    """)

    # Create Recurring Events Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recurring_events (
            id SERIAL PRIMARY KEY,
            event_name TEXT NOT NULL,
            frequency TEXT NOT NULL,
            day_of_week INT,         
            time_of_day TIME         
        )
    """)

    # Create Unqiue Events Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unique_events (
            id SERIAL PRIMARY KEY,
            event_name TEXT NOT NULL,
            version VARCHAR(20),
            start_datetime  TIMESTAMP NOT NULL,
            end_datetime    TIMESTAMP NOT NULL
        )
    """)

    print("Tables Initialized...")
    conn.commit()
    cur.close()
    conn.close()

def add_price_to_database(card_id, chart_data):
    """
    Inserts price history for a card.
    chart_data: list of dicts with keys: x (datetime), y, series_name
    """
    conn = get_connection()
    try:
        values = []
        for point in chart_data:
            pc_value = point['y'] if point['series_name'].lower() == 'pc' else None
            console_value = point['y'] if point['series_name'].lower() in ['ps', 'console'] else None
            values.append((card_id, pc_value, console_value, point['x']))

        sql = """
        INSERT INTO price_history (card_id, pc_value, console_value, date_time)
        VALUES (%s, %s, %s, %s)
        """
        with conn.cursor() as cur:
            for v in values:
                cur.execute(sql, v)

        conn.commit()
    finally:
        conn.close()


def insert_card(card_id, card_details):
    """
    Inserts or updates a single card in the cards table.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cards (
                    id, name, game, version, nationality, league, club,
                    rating, weak_foot, skill_move, height, accelerate
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    game = EXCLUDED.game,
                    version = EXCLUDED.version,
                    nationality = EXCLUDED.nationality,
                    league = EXCLUDED.league,
                    club = EXCLUDED.club,
                    rating = EXCLUDED.rating,
                    weak_foot = EXCLUDED.weak_foot,
                    skill_move = EXCLUDED.skill_move,
                    height = EXCLUDED.height,
                    accelerate = EXCLUDED.accelerate;
            """, (
                card_id,
                card_details.get("name"),
                card_details.get("game"),
                card_details.get("version"),
                card_details.get("nation"),
                card_details.get("league"),
                card_details.get("club"),
                card_details.get("rating"),
                card_details.get("weakfoot"),
                card_details.get("skills"),
                card_details.get("height"),
                card_details.get("accelerate")
            ))
        conn.commit()
    finally:
        conn.close()


def insert_card_roles(card_id, roles):
    """
    Insert or update card roles.
    roles: list of dicts with keys: position, role, plus
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for r in roles:
                # Check if this card_id + position already exists
                cur.execute("""
                    SELECT 1 FROM card_roles
                    WHERE card_id = %s AND position = %s AND role = %s
                """, (card_id, r.get("position"), r.get("role")))
                exists = cur.fetchone() is not None

                if exists:
                    continue
                else:
                    # Insert new row
                    cur.execute("""
                        INSERT INTO card_roles (card_id, position, role, plus)
                        VALUES (%s, %s, %s, %s)
                    """, (card_id, r.get("position"), r.get("role"), r.get("plus")))
            conn.commit()
    finally:
        conn.close()


def insert_card_playstyles(card_id, playstyles_list):
    """
    Insert or update playstyles for a card.
    playstyles_list: list of dicts with keys: playstyle, plus
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for ps in playstyles_list:
                cur.execute("""
                    INSERT INTO card_playstyles (card_id, playstyle, plus)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (card_id, playstyle) DO UPDATE SET
                        plus = EXCLUDED.plus;
                """, (
                    card_id,
                    ps.get("playstyle"),
                    ps.get("plus")
                ))
        conn.commit()
    finally:
        conn.close()


def insert_card_stats(card_id, stats_list):
    """
    Insert or update card stats for each category.
    
    stats_list: dict with keys like 'pace', 'shooting', etc., each containing a dict of substats
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:

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
                if not isinstance(substats, dict) or not substats:
                    continue

                columns = list(substats.keys())
                values = list(substats.values())

                # Check if a row for this card already exists
                cur.execute(f"SELECT 1 FROM {table} WHERE card_id = %s", (card_id,))
                exists = cur.fetchone() is not None

                if exists:
                    # Build update set dynamically
                    set_clause = ", ".join([f"{col} = %s" for col in columns])
                    cur.execute(
                        f"UPDATE {table} SET {set_clause} WHERE card_id = %s",
                        values + [card_id]
                    )
                else:
                    # Insert new row
                    all_columns = ["card_id"] + columns
                    placeholders = ", ".join(["%s"] * len(all_columns))
                    cur.execute(
                        f"INSERT INTO {table} ({', '.join(all_columns)}) VALUES ({placeholders})",
                        [card_id] + values
                    )

            conn.commit()
    finally:
        conn.close()




def drop_all_tables():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("All tables dropped.")

# drop_all_tables()
# initcardTable()