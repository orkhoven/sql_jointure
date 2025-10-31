# streamlit_sql_practice.py
import streamlit as st
import sqlite3
import pandas as pd
import re, base64, requests, json, os
from PIL import Image, ImageDraw
from contextlib import closing

st.set_page_config(page_title="Pratique SQL : Livres & Films", layout="wide")

# --- GitHub configuration ---
REPO = "orkhoven/sql_panda"
BRANCH = "main"
SUBDIR = "submissions"
TOKEN_SECRET_KEY = "GITHUB_TOKEN"

# --- Database setup ---
DEFAULT_SQL = r"""
PRAGMA foreign_keys = OFF;
DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS directors;
PRAGMA foreign_keys = ON;
CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT, country TEXT, birth_year INTEGER);
CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER, year INTEGER, genre TEXT, rating NUMERIC, pages INTEGER, FOREIGN KEY (author_id) REFERENCES authors(id));
CREATE TABLE directors (id INTEGER PRIMARY KEY, name TEXT, country TEXT, birth_year INTEGER);
CREATE TABLE movies (id INTEGER PRIMARY KEY, title TEXT, director_id INTEGER, year INTEGER, genre TEXT, rating NUMERIC, duration_minutes INTEGER, FOREIGN KEY (director_id) REFERENCES directors(id));
INSERT INTO authors VALUES
(1,'Ava Martin','USA',1975),(2,'Luca Moretti','Italy',1969),(3,'Priya Sharma','India',1982),(4,'Hiro Tanaka','Japan',1970),
(5,'Sofia Garcia','Spain',1988),(6,'Thomas Müller','Germany',1965),(7,'Emily O''Connor','Ireland',1990),(8,'Ahmed El-Sayed','Egypt',1978),
(9,'Claire Dupont','France',1980),(10,'Jacob Svensson','Sweden',1973),(11,'Maya Patel','UK',1992);
INSERT INTO books VALUES
(1,'Stars Over Cairo',8,2015,'Drame',4.3,320),(2,'The Last Algorithm',2,2020,'Sci-Fi',4.7,412),
(3,'Whispers in Winter',9,2012,'Romance',3.9,256),(4,'Gardens of Kyoto',4,2018,'Mystère',4.1,288),
(5,'Midnight Circus',6,2005,'Fantaisie',4.0,360),(6,'Echoes of Tomorrow',3,2021,'Sci-Fi',4.8,488),
(7,'The Quiet Harbor',1,2010,'Drame',3.7,220),(8,'A Walk Through Lisbon',5,2016,'Romance',4.2,304),
(9,'Code of Silence',10,2008,'Thriller',4.5,340),(10,'The Paper Garden',7,2019,'Mystère',3.8,272),
(11,'Notes from the Train',11,2022,'Drame',4.6,198);
INSERT INTO directors VALUES
(1,'Martin Ruiz','Mexico',1968),(2,'Claire Legrand','France',1976),(3,'Kenji Sato','Japan',1981),
(4,'Olivia Bennett','UK',1980),(5,'Diego Fernandez','Spain',1972),(6,'Anna Kowalska','Poland',1974),
(7,'Marcus Brown','USA',1965),(8,'Li Wei','China',1979),(9,'Nina Rossi','Italy',1984),
(10,'Sven Larsson','Sweden',1970),(11,'Rana Habib','Lebanon',1986);
INSERT INTO movies VALUES
(1,'Nightfall in Rome',9,2011,'Drame',7.2,112),(2,'Quantum Blossom',3,2020,'Sci-Fi',8.6,135),
(3,'The Lovers'' Map',2,2016,'Romance',6.9,101),(4,'Hidden Library',4,2019,'Mystère',7.8,124),
(5,'Paper Clouds',5,2007,'Fantaisie',7.0,128),(6,'Silent Protocol',7,2009,'Thriller',8.1,140),
(7,'Harbor Lights',1,2013,'Drame',6.5,96),(8,'Tomorrow''s Song',8,2021,'Sci-Fi',9.0,148),
(9,'Lisbon Afternoon',5,2015,'Romance',7.4,103),(10,'Midnight Train',10,2003,'Mystère',6.8,115),
(11,'City of Paper',11,2018,'Drame',7.6,122);
"""

# --- Exercise set ---
SOL = {
    1: "SELECT title, year FROM books;",
    2: "SELECT title, year FROM movies WHERE genre='Sci-Fi';",
    3: "SELECT title, author_id, rating FROM books WHERE rating>=4.5;",
    4: "SELECT title, year FROM movies WHERE year BETWEEN 2015 AND 2021;",
    5: "SELECT title, pages, rating FROM books WHERE pages BETWEEN 250 AND 400 AND rating>4.0;",
    6: "SELECT title, rating FROM movies ORDER BY rating DESC LIMIT 5;",
    7: "SELECT title, genre, year FROM books WHERE genre IN('Drame','Romance');",
    8: "SELECT title, genre, rating FROM movies WHERE genre IN('Mystère','Thriller') AND rating>=7.0;",
    9: "SELECT title, year FROM books WHERE year<2010;",
    10: "SELECT title, duration_minutes, genre FROM movies WHERE duration_minutes BETWEEN 100 AND 130;",
    11: "SELECT title, year FROM books ORDER BY year DESC LIMIT 3;",
    12: "SELECT title, rating, pages FROM books WHERE rating<4.0 OR pages<250;",
    13: "SELECT title, year, rating FROM movies WHERE year IN(2019,2020);",
    14: "SELECT * FROM books WHERE author_id IN(1,3,5) AND rating>=4.0;",
    15: "SELECT * FROM movies ORDER BY genre ASC, rating DESC;",
    16: "SELECT b.title,a.name,a.country FROM books b JOIN authors a ON a.id=b.author_id;",
    17: "SELECT m.title,d.name FROM movies m LEFT JOIN directors d ON d.id=m.director_id;",
    18: "SELECT a.name,COUNT(b.id) AS total FROM authors a LEFT JOIN books b ON b.author_id=a.id GROUP BY a.name;",
    19: "SELECT d.name,AVG(m.rating) AS moyenne FROM directors d JOIN movies m ON m.director_id=d.id GROUP BY d.name;",
    20: ("WITH a AS (SELECT country,COUNT(*) AS total_auteurs FROM authors GROUP BY country), "
         "d AS (SELECT country,COUNT(*) AS total_realisateurs FROM directors GROUP BY country), "
         "all_c AS (SELECT country FROM a UNION SELECT country FROM d) "
         "SELECT all_c.country,COALESCE(a.total_auteurs,0) AS total_auteurs,COALESCE(d.total_realisateurs,0) AS total_realisateurs "
         "FROM all_c LEFT JOIN a ON a.country=all_c.country LEFT JOIN d ON d.country=all_c.country;")
}

# --- DB helpers ---
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def reset_db(c, s):
    with closing(c.cursor()) as cur:
        cur.executescript(s)
    c.commit()

def run_sql(c, q):
    with closing(c.cursor()) as cur:
        cur.execute(q)
        if re.match(r"\s*(WITH|SELECT|PRAGMA)\b", q.strip(), re.I):
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols), None
        c.commit()
        return None, "OK"

# --- Visual progress ---
def render_bar():
    cols = st.columns(len(SOL))
    for i, col in enumerate(cols):
        color = "#cccccc"
        if st.session_state.status[i] == "solved":
            color = "#2ecc71"
        elif st.session_state.status[i] == "skipped":
            color = "#e67e22"
        with col:
            st.markdown(
                f"<div style='background-color:{color};text-align:center;border-radius:3px;padding:4px;color:white;'>{i+1}</div>",
                unsafe_allow_html=True,
            )

def save_progress_image(name):
    w, h = 400, 40
    sw = w // len(SOL)
    img = Image.new("RGB", (w, h), "white")
    dr = ImageDraw.Draw(img)
    for i, s in enumerate(st.session_state.status):
        c = "#ccc"
        if s == "solved":
            c = "#2ecc71"
        elif s == "skipped":
            c = "#e67e22"
        dr.rectangle([i * sw, 0, (i + 1) * sw - 2, h], fill=c, outline="black")
    fname = f"{name}_progress.png"
    img.save(fname)
    return fname

def upload_git(f, repo, token, msg, branch="main"):
    fn = os.path.basename(f)
    url = f"https://api.github.com/repos/{repo}/contents/{SUBDIR}/{fn}"
    with open(f, "rb") as fh:
        content = base64.b64encode(fh.read()).decode("utf-8")
    headers = {"Authorization": f"token {token}"}
    get_resp = requests.get(url, headers=headers)
    sha = get_resp.json().get("sha") if get_resp.status_code == 200 else None
    data = {"message": msg, "content": content, "branch": branch}
    if sha:
        data["sha"] = sha
    put_resp = requests.put(url, headers=headers, data=json.dumps(data))
    return put_resp.status_code

# --- Initialize session ---
if "status" not in st.session_state:
    st.session_state.status = ["locked"] * len(SOL)
    st.session_state.inputs = [""] * len(SOL)
    st.session_state.step = 0

conn = get_conn()
reset_db(conn, DEFAULT_SQL)

st.title("Pratique SQL — Livres & Films")
render_bar()

i = st.session_state.step
st.subheader(f"Exercice {i+1}")
st.code(SOL[i+1], language="sql")

user = st.text_area("Votre requête SQL :", st.session_state.inputs[i], height=150)

col1, col2, col3 = st.columns(3)
run = col1.button("Exécuter")
skip = col2.button("Je bloque — voir la solution")
reset = col3.button("Réinitialiser la base")

if reset:
    reset_db(conn, DEFAULT_SQL)
    st.success("Base réinitialisée.")

if skip:
    st.session_state.status[i] = "skipped"
    st.session_state.inputs[i] = SOL[i+1]
    st.session_state.step = min(i + 1, len(SOL) - 1)
    st.rerun()

if run:
    query = user.strip()
    if not query:
        st.error("Veuillez saisir une requête SQL.")
    else:
        st.session_state.inputs[i] = query
        try:
            df, msg = run_sql(conn, query)
            if df is not None and not df.empty:
                st.dataframe(df, use_container_width=True)
                st.session_state.status[i] = "solved"
                if i < len(SOL) - 1:
                    st.session_state.step = i + 1
                st.rerun()
            elif msg:
                st.success(msg)
            else:
                st.warning("Résultat vide.")
        except Exception as e:
            st.error(f"Erreur SQL : {e}")

st.markdown("---")
st.subheader("Soumission de votre progression")

name = st.text_input("Nom complet :")
if st.button("Envoyer à l’enseignant"):
    if not name.strip():
        st.error("Nom manquant.")
    else:
        img_file = save_progress_image(name.replace(" ", "_"))
        df = pd.DataFrame({
            "Exercice": list(SOL.keys()),
            "Réponse": st.session_state.inputs,
            "Statut": st.session_state.status
        })
        csv_file = f"{name.replace(' ', '_')}_answers.csv"
        df.to_csv(csv_file, index=False)

        if TOKEN_SECRET_KEY not in st.secrets:
            st.error("Secret GITHUB_TOKEN introuvable.")
        else:
            token = st.secrets[TOKEN_SECRET_KEY]
            code1 = upload_git(img_file, REPO, token, f"Progress {name}")
            code2 = upload_git(csv_file, REPO, token, f"Answers {name}")
            if code1 in [200, 201] and code2 in [200, 201]:
                st.success("Fichiers envoyés avec succès vers GitHub /submissions.")
            else:
                st.error(f"Erreur d’envoi : codes {code1}, {code2}")
