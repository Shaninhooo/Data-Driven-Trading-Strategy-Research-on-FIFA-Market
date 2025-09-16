import psycopg
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

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
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            game INT,
            version VARCHAR(20),
            nationality VARCHAR(20),
            league VARCHAR(20),
            club VARCHAR(20),
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
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            playstyle_name VARCHAR(50) NOT NULL,
            plus BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY(card_id, playstyle_name)
        )
    """)

    # Create card Roles Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_roles (
            card_id INT REFERENCES cards(id) ON DELETE CASCADE,
            role_name VARCHAR(50) NOT NULL,
            position VARCHAR(50) NOT NULL, 
            plus SMALLINT DEFAULT 1,
            PRIMARY KEY(card_id, role_name)
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
            volley INT,
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
            fk_accuracy INT,
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
            date_time  TIMESTAMP NOT NULL,
            PRIMARY KEY(card_id, date_time)
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

def addPricetoDatabase(card_id, chart_data):
    conn = get_connection()
    # Prepare list of tuples
    values = []
    for point in chart_data:
        pc_value = point['y'] if point['series_name'].lower() == 'pc' else None
        console_value = point['y'] if point['series_name'].lower() in ['ps', 'console'] else None
        values.append((card_id, pc_value, console_value, point['x']))

    sql = """
    INSERT INTO price_history (card_id, pc_value, console_value, date_time)
    VALUES %s
    ON CONFLICT (card_id, date_time)
    DO UPDATE SET
        pc_value = COALESCE(EXCLUDED.pc_value, price_history.pc_value),
        console_value = COALESCE(EXCLUDED.console_value, price_history.console_value)
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

def insert_card(card_id, card_details):
    """
    Inserts a single card into the cards table.

    card_id: int
    card_details: dict with keys like name, game, version, nationality, league, club, rating, weak_foot, skill_move, height, accelerate
    conn: psycopg2 connection
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cards (
                    id, name, game, version, nationality, league, club, rating, weak_foot, skill_move, height, accelerate
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

def insert_card_roles(id, roles_list):
    """
    roles_list: list of dicts with keys:
    card_id, role
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO card_roles (card_id, role_name, position, plus)
            VALUES %s
            ON CONFLICT (card_id, role_name, position, plus) DO NOTHING
            """
            values = [(id, r['role'], r['position'], r['plus']) for r in roles_list]
            execute_values(cur, sql, values)
        conn.commit()
    finally:
        conn.close()


def insert_card_playstyles(id, playstyles_list):
    """
    playstyles_list: list of dicts with keys:
    card_id, playstyle
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO card_playstyles (card_id, playstyle, plus)
            VALUES %s
            ON CONFLICT (card_id, playstyle) DO NOTHING
            """
            values = [(id, p['playstyle'], p['plus']) for p in playstyles_list]
            execute_values(cur, sql, values)
        conn.commit()
    finally:
        conn.close()

def insert_card_stats(card_id, stats_list):
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
                stats_values = {k.lower(): v for k, v in stats_list.get(category, {}).items()}
                if stats_values:
                    # Build columns and values dynamically
                    columns = ["card_id"] + list(stats_values.keys())
                    values = [card_id] + list(stats_values.values())
                    placeholders = ", ".join(["%s"] * len(values))
                    cur.execute(f"""
                        INSERT INTO {table} ({", ".join(columns)})
                        VALUES ({placeholders})
                    """, values)
            
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
