
import streamlit as st
import sqlite3
import pandas as pd
import re
from contextlib import closing

st.set_page_config(page_title="SQL Practice: Books & Movies", layout="wide")

# ---------- Default schema/data (adapted to SQLite) ----------
DEFAULT_SQL = r"""
-- Clean start (SQLite variant)
PRAGMA foreign_keys = OFF;
DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS directors;
PRAGMA foreign_keys = ON;

-- Authors
CREATE TABLE authors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  birth_year INTEGER
);

-- Books
CREATE TABLE books (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  author_id INTEGER,
  year INTEGER,
  genre TEXT,
  rating NUMERIC,
  pages INTEGER,
  FOREIGN KEY (author_id) REFERENCES authors(id)
);

-- Directors
CREATE TABLE directors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  birth_year INTEGER
);

-- Movies
CREATE TABLE movies (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  director_id INTEGER,
  year INTEGER,
  genre TEXT,
  rating NUMERIC,
  duration_minutes INTEGER,
  FOREIGN KEY (director_id) REFERENCES directors(id)
);

-- Data: authors (11)
INSERT INTO authors (id, name, country, birth_year) VALUES
(1,'Ava Martin','USA',1975),
(2,'Luca Moretti','Italy',1969),
(3,'Priya Sharma','India',1982),
(4,'Hiro Tanaka','Japan',1970),
(5,'Sofia Garcia','Spain',1988),
(6,'Thomas Müller','Germany',1965),
(7,'Emily O''Connor','Ireland',1990),
(8,'Ahmed El-Sayed','Egypt',1978),
(9,'Claire Dupont','France',1980),
(10,'Jacob Svensson','Sweden',1973),
(11,'Maya Patel','UK',1992);

-- Data: books (11)
INSERT INTO books (id, title, author_id, year, genre, rating, pages) VALUES
(1,'Stars Over Cairo',8,2015,'Drama',4.3,320),
(2,'The Last Algorithm',2,2020,'Sci-Fi',4.7,412),
(3,'Whispers in Winter',9,2012,'Romance',3.9,256),
(4,'Gardens of Kyoto',4,2018,'Mystery',4.1,288),
(5,'Midnight Circus',6,2005,'Fantasy',4.0,360),
(6,'Echoes of Tomorrow',3,2021,'Sci-Fi',4.8,488),
(7,'The Quiet Harbor',1,2010,'Drama',3.7,220),
(8,'A Walk Through Lisbon',5,2016,'Romance',4.2,304),
(9,'Code of Silence',10,2008,'Thriller',4.5,340),
(10,'The Paper Garden',7,2019,'Mystery',3.8,272),
(11,'Notes from the Train',11,2022,'Drama',4.6,198);

-- Data: directors (11)
INSERT INTO directors (id, name, country, birth_year) VALUES
(1,'Martin Ruiz','Mexico',1968),
(2,'Claire Legrand','France',1976),
(3,'Kenji Sato','Japan',1981),
(4,'Olivia Bennett','UK',1980),
(5,'Diego Fernandez','Spain',1972),
(6,'Anna Kowalska','Poland',1974),
(7,'Marcus Brown','USA',1965),
(8,'Li Wei','China',1979),
(9,'Nina Rossi','Italy',1984),
(10,'Sven Larsson','Sweden',1970),
(11,'Rana Habib','Lebanon',1986);

-- Data: movies (11)
INSERT INTO movies (id, title, director_id, year, genre, rating, duration_minutes) VALUES
(1,'Nightfall in Rome',9,2011,'Drama',7.2,112),
(2,'Quantum Blossom',3,2020,'Sci-Fi',8.6,135),
(3,'The Lovers'' Map',2,2016,'Romance',6.9,101),
(4,'Hidden Library',4,2019,'Mystery',7.8,124),
(5,'Paper Clouds',5,2007,'Fantasy',7.0,128),
(6,'Silent Protocol',7,2009,'Thriller',8.1,140),
(7,'Harbor Lights',1,2013,'Drama',6.5,96),
(8,'Tomorrow''s Song',8,2021,'Sci-Fi',9.0,148),
(9,'Lisbon Afternoon',5,2015,'Romance',7.4,103),
(10,'Midnight Train',10,2003,'Mystery',6.8,115),
(11,'City of Paper',11,2018,'Drama',7.6,122);
"""

EXERCISES = [
    "1) List all book titles and their years (ORDER BY year ASC).",
    "2) Movies with genre='Sci-Fi' (title, year).",
    "3) Books with rating >= 4.5 (title, author_id, rating) ORDER BY rating DESC.",
    "4) Movies released BETWEEN 2015 AND 2021 (title, year, director_id).",
    "5) Books pages BETWEEN 250 AND 400 AND rating > 4.0 (title, pages, rating).",
    "6) Top 5 highest-rated movies (title, rating) ORDER BY rating DESC LIMIT 5.",
    "7) Books where genre='Drama' OR 'Romance' (title, genre, year).",
    "8) Movies where (genre='Mystery' OR 'Thriller') AND rating >= 7.0 (title, genre, rating).",
    "9) Books before 2010 (title, year) ORDER BY year DESC.",
    "10) Movies with duration BETWEEN 100 AND 130 (title, duration_minutes, genre).",
    "11) 3 most recent books (title, year) LIMIT 3.",
    "12) Books with rating < 4.0 OR pages < 250 (title, rating, pages).",
    "13) Movies where year IN (2019, 2020) (title, year, rating).",
    "14) Books with author_id IN (1,3,5) AND rating >= 4.0.",
    "15) All movies ORDER BY genre ASC, rating DESC.",
    "16) List all book titles with the author name and country.",
    "17) Show all movies with their director name, including movies with no matching director (LEFT JOIN).",
    "18) For each author, show the number of books (include authors with zero).",
    "19) For each director, show the average rating of their movies (only where at least one movie exists).",
    "20) By country, show counts of authors and directors (simulate FULL JOIN).",
]

# ---------- Database helpers ----------
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def reset_db(conn, sql_script: str):
    # Strip MySQL-specific statements if pasted
    script = re.sub(r"SET\s+FOREIGN_KEY_CHECKS\s*=\s*\d+;\s*", "", sql_script, flags=re.IGNORECASE)
    with closing(conn.cursor()) as cur:
        cur.executescript(script)
    conn.commit()

def run_sql(conn, sql: str):
    with closing(conn.cursor()) as cur:
        cur.execute(sql)
        if re.match(r"\s*(WITH\b|SELECT\b|PRAGMA\b)", sql.strip(), flags=re.IGNORECASE):
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=cols)
            return df, None
        else:
            conn.commit()
            return None, f"OK, {cur.rowcount if cur.rowcount != -1 else 0} row(s) affected."

# ---------- UI ----------
st.title("SQL Practice — Books & Movies (SQLite in Streamlit)")

# Sidebar
with st.sidebar:
    st.header("Database")
    if "initialized" not in st.session_state:
        st.session_state.initialized = False

    default_btn = st.button("Initialize / Reset with default data", use_container_width=True)
    custom_sql = st.text_area("Or paste your own schema + data (optional)", height=220, value=DEFAULT_SQL)
    custom_btn = st.button("Reset using the script above", use_container_width=True)

conn = get_conn()

if not st.session_state.initialized or default_btn:
    reset_db(conn, DEFAULT_SQL)
    st.session_state.initialized = True
elif custom_btn:
    reset_db(conn, custom_sql)
    st.session_state.initialized = True

tabs = st.tabs(["Data Preview", "Exercises", "SQL Console"])

with tabs[0]:
    st.subheader("Tables")
    for tbl in ["authors", "books", "directors", "movies"]:
        st.markdown(f"**{tbl}**")
        try:
            df, _ = run_sql(conn, f"SELECT * FROM {tbl} LIMIT 50;")
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(str(e))

with tabs[1]:
    st.subheader("Exercises")
    idx = st.selectbox("Pick an exercise", options=list(range(1, len(EXERCISES)+1)), format_func=lambda i: EXERCISES[i-1])
    st.write(EXERCISES[idx-1])

    presets = {
        16: "SELECT b.title, a.name AS author_name, a.country\nFROM books b\nJOIN authors a ON a.id = b.author_id;",
        17: "SELECT m.title, d.name AS director_name\nFROM movies m\nLEFT JOIN directors d ON d.id = m.director_id;",
        18: "SELECT a.name, COUNT(b.id) AS total_books\nFROM authors a\nLEFT JOIN books b ON b.author_id = a.id\nGROUP BY a.name;",
        19: "SELECT d.name, AVG(m.rating) AS avg_rating\nFROM directors d\nJOIN movies m ON m.director_id = d.id\nGROUP BY d.name;",
        20: ("-- FULL JOIN simulation by country (SQLite-compatible)\n"
             "WITH a AS (\n  SELECT country, COUNT(*) AS total_authors FROM authors GROUP BY country\n),\n"
             "d AS (\n  SELECT country, COUNT(*) AS total_directors FROM directors GROUP BY country\n),\n"
             "all_c AS (\n  SELECT country FROM a\n  UNION\n  SELECT country FROM d\n)\n"
             "SELECT all_c.country,\n"
             "       COALESCE(a.total_authors, 0) AS total_authors,\n"
             "       COALESCE(d.total_directors, 0) AS total_directors\n"
             "FROM all_c\nLEFT JOIN a ON a.country = all_c.country\n"
             "LEFT JOIN d ON d.country = all_c.country\nORDER BY all_c.country;"),
    }
    default_sql = presets.get(idx, "-- Write your query here")
    user_query = st.text_area("Your SQL", value=default_sql, height=220)

    col1, col2 = st.columns([1,1])
    with col1:
        run_ex = st.button("Run query", type="primary", use_container_width=True)
    with col2:
        reset_btn = st.button("Reset database", use_container_width=True)

    if reset_btn:
        reset_db(conn, DEFAULT_SQL)
        st.success("Database reset.")

    if run_ex:
        try:
            df, msg = run_sql(conn, user_query)
            if df is not None:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success(msg or "OK")
        except Exception as e:
            st.error(str(e))

with tabs[2]:
    st.subheader("SQL Console")
    free_sql = st.text_area("Enter any SQL to run against the current database", height=180, placeholder="SELECT * FROM books LIMIT 10;")
    if st.button("Execute", type="primary"):
        try:
            df, msg = run_sql(conn, free_sql)
            if df is not None:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success(msg or "OK")
        except Exception as e:
            st.error(str(e))

st.caption("SQLite engine inside Streamlit. MySQL FULL JOIN is emulated via UNION/LEFT JOIN.")
