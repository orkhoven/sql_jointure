import streamlit as st
import sqlite3
import pandas as pd
import re, base64, requests
from PIL import Image, ImageDraw
from contextlib import closing

# === CONFIGURATION ===
st.set_page_config(page_title="Pratique SQL : Livres & Films", layout="wide")

# === BASE DE DONNÉES PAR DÉFAUT ===
DEFAULT_SQL = r"""
PRAGMA foreign_keys = OFF;
DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS directors;
PRAGMA foreign_keys = ON;

-- Auteurs
CREATE TABLE authors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  birth_year INTEGER
);

-- Livres
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

-- Réalisateurs
CREATE TABLE directors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  birth_year INTEGER
);

-- Films
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

-- Données : auteurs
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

-- Données : livres
INSERT INTO books (id, title, author_id, year, genre, rating, pages) VALUES
(1,'Stars Over Cairo',8,2015,'Drame',4.3,320),
(2,'The Last Algorithm',2,2020,'Sci-Fi',4.7,412),
(3,'Whispers in Winter',9,2012,'Romance',3.9,256),
(4,'Gardens of Kyoto',4,2018,'Mystère',4.1,288),
(5,'Midnight Circus',6,2005,'Fantaisie',4.0,360),
(6,'Echoes of Tomorrow',3,2021,'Sci-Fi',4.8,488),
(7,'The Quiet Harbor',1,2010,'Drame',3.7,220),
(8,'A Walk Through Lisbon',5,2016,'Romance',4.2,304),
(9,'Code of Silence',10,2008,'Thriller',4.5,340),
(10,'The Paper Garden',7,2019,'Mystère',3.8,272),
(11,'Notes from the Train',11,2022,'Drame',4.6,198);

-- Données : réalisateurs
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

-- Données : films
INSERT INTO movies (id, title, director_id, year, genre, rating, duration_minutes) VALUES
(1,'Nightfall in Rome',9,2011,'Drame',7.2,112),
(2,'Quantum Blossom',3,2020,'Sci-Fi',8.6,135),
(3,'The Lovers'' Map',2,2016,'Romance',6.9,101),
(4,'Hidden Library',4,2019,'Mystère',7.8,124),
(5,'Paper Clouds',5,2007,'Fantaisie',7.0,128),
(6,'Silent Protocol',7,2009,'Thriller',8.1,140),
(7,'Harbor Lights',1,2013,'Drame',6.5,96),
(8,'Tomorrow''s Song',8,2021,'Sci-Fi',9.0,148),
(9,'Lisbon Afternoon',5,2015,'Romance',7.4,103),
(10,'Midnight Train',10,2003,'Mystère',6.8,115),
(11,'City of Paper',11,2018,'Drame',7.6,122);
"""

# === EXERCICES ===
EXERCICES = [
    "1) Lister tous les titres de livres et leurs années (ORDER BY year ASC).",
    "2) Films dont le genre est 'Sci-Fi' (title, year).",
    "3) Livres avec une note >= 4.5 (title, author_id, rating) ORDER BY rating DESC.",
    "4) Films sortis ENTRE 2015 ET 2021 (title, year, director_id).",
    "5) Livres avec un nombre de pages ENTRE 250 ET 400 ET une note > 4.0 (title, pages, rating).",
    "6) Top 5 des films les mieux notés (title, rating) ORDER BY rating DESC LIMIT 5.",
    "7) Livres où genre='Drame' OU 'Romance' (title, genre, year).",
    "8) Films où (genre='Mystère' OU 'Thriller') ET rating >= 7.0 (title, genre, rating).",
    "9) Livres publiés avant 2010 (title, year) ORDER BY year DESC.",
    "10) Films dont la durée est ENTRE 100 ET 130 minutes (title, duration_minutes, genre).",
    "11) Les 3 livres les plus récents (title, year) LIMIT 3.",
    "12) Livres avec rating < 4.0 OU pages < 250 (title, rating, pages).",
    "13) Films dont l’année est dans (2019, 2020) (title, year, rating).",
    "14) Livres avec author_id IN (1,3,5) ET rating >= 4.0.",
    "15) Tous les films classés par genre ASC, rating DESC.",
    "16) Lister tous les livres avec le nom et le pays de leur auteur.",
    "17) Afficher tous les films avec le nom du réalisateur, y compris ceux sans réalisateur (LEFT JOIN).",
    "18) Pour chaque auteur, afficher le nombre de livres (inclure ceux sans livre).",
    "19) Pour chaque réalisateur, afficher la note moyenne de ses films (uniquement ceux ayant au moins un film).",
    "20) Par pays, afficher le nombre d’auteurs et de réalisateurs (simuler un FULL JOIN). 🔶 *Indice : SQLite ne gère pas le FULL JOIN — il est ici simulé avec UNION et LEFT JOIN.*"
]

PRESETS = {
    16: "SELECT b.title, a.name AS auteur, a.country FROM books b JOIN authors a ON a.id = b.author_id;",
    17: "SELECT m.title, d.name AS realisateur FROM movies m LEFT JOIN directors d ON d.id = m.director_id;",
    18: "SELECT a.name, COUNT(b.id) AS total_livres FROM authors a LEFT JOIN books b ON b.author_id = a.id GROUP BY a.name;",
    19: "SELECT d.name, AVG(m.rating) AS moyenne_note FROM directors d JOIN movies m ON m.director_id = d.id GROUP BY d.name;",
    20: ("-- Simulation de FULL JOIN (SQLite)\n"
         "WITH a AS (SELECT country, COUNT(*) AS total_auteurs FROM authors GROUP BY country),\n"
         "d AS (SELECT country, COUNT(*) AS total_realisateurs FROM directors GROUP BY country),\n"
         "all_c AS (SELECT country FROM a UNION SELECT country FROM d)\n"
         "SELECT all_c.country, COALESCE(a.total_auteurs,0) AS total_auteurs,\n"
         "COALESCE(d.total_realisateurs,0) AS total_realisateurs\n"
         "FROM all_c LEFT JOIN a ON a.country=all_c.country\n"
         "LEFT JOIN d ON d.country=all_c.country ORDER BY all_c.country;")
}

# === OUTILS BD ===
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def reset_db(conn, sql_script):
    script = re.sub(r"SET\s+FOREIGN_KEY_CHECKS\s*=\s*\d+;\s*", "", sql_script, flags=re.IGNORECASE)
    with closing(conn.cursor()) as cur:
        cur.executescript(script)
    conn.commit()

def run_sql(conn, sql):
    with closing(conn.cursor()) as cur:
        cur.execute(sql)
        if re.match(r"\s*(WITH\b|SELECT\b|PRAGMA\b)", sql.strip(), flags=re.IGNORECASE):
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols), None
        conn.commit()
        return None, "OK"

# === PROGRESSION ===
def render_progress_bar():
    html = '<div style="display:flex;gap:4px;margin:10px 0;">'
    for s in st.session_state.status:
        color = "#ccc"
        if s == "solved":
            color = "#2ecc71"
        elif s == "skipped":
            color = "#e67e22"
        html += f'<div style="flex:1;height:20px;background-color:{color};border-radius:3px;"></div>'
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)

def save_progress_image(student_name):
    width, height = 400, 40
    step_width = width // len(st.session_state.status)
    bar = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(bar)
    for i, s in enumerate(st.session_state.status):
        color = "#cccccc"
        if s == "solved":
            color = "#2ecc71"
        elif s == "skipped":
            color = "#e67e22"
        draw.rectangle([i * step_width, 0, (i + 1) * step_width - 2, height], fill=color, outline="black")
    file = f"{student_name}_progress.png"
    bar.save(file)
    return file

def upload_to_github(file_path, repo, token, message=None):
    filename = file_path.split("/")[-1]
    url = f"https://api.github.com/repos/{repo}/contents/submissions/{filename}"
    with open(file_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")
    headers = {"Authorization": f"token {token}"}
    data = {"message": message or f"Add {filename}", "content": content}
    r = requests.put(url, headers=headers, json=data)
    if r.status_code not in [200, 201]:
        st.error(f"Erreur GitHub ({r.status_code}): {r.text}")
    else:
        st.info(f"✅ {filename} envoyé sur GitHub.")
    return r.status_code

# === ÉTAT INITIAL ===
if "status" not in st.session_state:
    st.session_state.status = ["locked"] * len(EXERCICES)
    st.session_state.step = 0
    st.session_state.inputs = [""] * len(EXERCICES)

conn = get_conn()
reset_db(conn, DEFAULT_SQL)

# === INTERFACE ===
st.title("Pratique SQL — Livres & Films (avec progression)")
render_progress_bar()

exo_index = st.session_state.step
exo_text = EXERCICES[exo_index]
st.subheader(f"Exercice {exo_index+1}")
st.write(exo_text)

user_query = st.text_area(
    "Votre requête SQL :",
    value=st.session_state.inputs[exo_index],
    height=220,
    placeholder="-- Écrivez votre requête ici"
)

c1, c2, c3 = st.columns([1,1,1])
with c1:
    run = st.button("Exécuter la requête")
with c2:
    skip = st.button("Je bloque — voir la solution")
with c3:
    reset = st.button("Réinitialiser la base")

if reset:
    reset_db(conn, DEFAULT_SQL)
    st.success("Base de données réinitialisée.")

if skip:
    st.session_state.status[exo_index] = "skipped"
    st.warning("Solution proposée :")
    solution = PRESETS.get(exo_index + 1, "-- Pas de solution prédéfinie pour cet exercice.")
    st.code(solution, language="sql")
    st.session_state.inputs[exo_index] = solution
    render_progress_bar()

if run:
    st.session_state.inputs[exo_index] = user_query
    try:
        df, msg = run_sql(conn, user_query)
        if df is not None:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.success("Requête exécutée avec succès.")
            st.session_state.status[exo_index] = "solved"
            if exo_index < len(EXERCICES) - 1:
                st.session_state.step += 1
                st.rerun()
            else:
                st.info("Tous les exercices sont terminés.")
        else:
            st.success(msg or "OK")
    except Exception as e:
        st.error(f"Erreur : {e}")

# === SOUMISSION ===
if all(s in ["solved", "skipped"] for s in st.session_state.status):
    st.markdown("---")
    st.subheader("Soumission de votre progression")
    name = st.text_input("Entrez votre nom complet :")
    if st.button("Envoyer à l’enseignant"):
        if name.strip():
            img_file = save_progress_image(name.strip().replace(" ", "_"))
            df = pd.DataFrame({
                "Exercice": EXERCICES,
                "Réponse": st.session_state.inputs,
                "Statut": st.session_state.status
            })
            csv_file = f"{name.strip().replace(' ', '_')}_answers.csv"
            df.to_csv(csv_file, index=False)
            token = st.secrets["GITHUB_TOKEN"]
            repo = "orkhoven/sql_panda"
            upload_to_github(img_file, repo, token, f"Progress for {name}")
            upload_to_github(csv_file, repo, token, f"Answers for {name}")
        else:
            st.error("Veuillez saisir votre nom avant d’envoyer.")
