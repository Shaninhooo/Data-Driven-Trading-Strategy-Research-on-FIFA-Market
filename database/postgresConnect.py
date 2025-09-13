import psycopg
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

cur = conn.cursor()


def initPlayerTable():
    # Create player table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            game INT,
            version VARCHAR(20),
            rating INT,
            weak_foot INT,
            skill_move INT,
            height INT,
            body_type VARCHAR(20),
            accelerate VARCHAR(20)
        )
    """)

    # Create Value History
    cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
            pc_value INT,
            console_value INT,
            date DATE,
            PRIMARY KEY(player_id, date)
        )
    """)

    # Create Player Playstyle Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_playstyles (
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
            playstyle_name VARCHAR(50) NOT NULL,
            category VARCHAR(50) NOT NULL, 
            level SMALLINT DEFAULT 1,
            PRIMARY KEY(player_id, playstyle_name)
        )
    """)

    # Create Player Playstyle Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_roles (
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
            role_name VARCHAR(50) NOT NULL,
            position VARCHAR(50) NOT NULL, 
            level SMALLINT DEFAULT 1,
            PRIMARY KEY(player_id, role_name)
        )
    """)
    
    # Create Pace Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_pace_stats (
            id SERIAL PRIMARY KEY,
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
            pace_overall INT,
            acceleration INT,
            sprint_speed INT
        )
    """)

    # Create Shooting Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_shooting_stats (
            id SERIAL PRIMARY KEY,
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
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
        CREATE TABLE IF NOT EXISTS player_passing_stats (
            id SERIAL PRIMARY KEY,
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
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
        CREATE TABLE IF NOT EXISTS player_dribbling_stats (
            id SERIAL PRIMARY KEY,
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
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
        CREATE TABLE IF NOT EXISTS player_defending_stats (
            id SERIAL PRIMARY KEY,
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
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
        CREATE TABLE IF NOT EXISTS player_physical_stats (
            id SERIAL PRIMARY KEY,
            player_id INT REFERENCES players(id) ON DELETE CASCADE,
            physical_overall INT,
            jumping INT,
            stamina INT,
            strength INT,
            aggression INT
        )
    """)

    print("Tables Initialized...")
    conn.commit()

try:
    initPlayerTable()
except Exception as e:
    print("Error initializing tables:", e)
    conn.rollback()
cur.close()
conn.close()