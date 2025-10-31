# streamlit_sql_practice.py
import streamlit as st
import sqlite3
import pandas as pd
import re, base64, requests, json
from PIL import Image, ImageDraw
from contextlib import closing

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Pratique SQL : Livres & Films", layout="wide")

REPO = "orkhoven/sql_panda"        # <-- votre dépôt GitHub
BRANCH = "main"                    # <-- "main" ou "master"
SUBDIR = "submissions"             # <-- dossier cible dans le dépôt
TOKEN_SECRET_KEY = "GITHUB_TOKEN"  # <-- clé de secret Streamlit

# =========================
# SQL PAR DÉFAUT (COMPLET)
# =========================
DEFAULT_SQL = r"""
PRAGMA foreign_keys = OFF;
DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS directors;
PRAGMA foreign_keys = ON;

CREATE TABLE authors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  birth_year INTEGER
);

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

CREATE TABLE directors (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  birth_year INTEGER
);

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

# =========================
# EXERCICES + SOLUTIONS + INDICES
# =========================
EXOS = [
    "1) Lister tous les titres de livres et leurs années.",
    "2) Films dont le genre est 'Sci-Fi'.",
    "3) Livres avec une note >= 4.5.",
    "4) Films sortis entre 2015 et 2021.",
    "5) Livres entre 250 et 400 pages et note > 4.0.",
    "6) Top 5 films par note.",
    "7) Livres 'Drame' ou 'Romance'.",
    "8) Films Mystère ou Thriller avec note >= 7.",
    "9) Livres avant 2010.",
    "10) Films entre 100 et 130 minutes.",
    "11) 3 livres les plus récents.",
    "12) Livres note < 4.0 OU pages < 250.",
    "13) Films année 2019 ou 2020.",
    "14) Livres auteurs 1,3,5 note >= 4.0.",
    "15) Tous les films triés par genre (ASC), note (DESC).",
    "16) Livres avec auteur et pays.",
    "17) Films + nom du réalisateur (LEFT JOIN).",
    "18) Nombre de livres par auteur (auteurs sans livre inclus).",
    "19) Moyenne des notes par réalisateur (au moins un film).",
    "20) Par pays, nombre d’auteurs et de réalisateurs (FULL JOIN simulé)."
]

SOL = {
    1:"SELECT title, year FROM books;",
    2:"SELECT title, year FROM movies WHERE genre='Sci-Fi';",
    3:"SELECT title, author_id, rating FROM books WHERE rating >= 4.5;",
    4:"SELECT title, year FROM movies WHERE year BETWEEN 2015 AND 2021;",
    5:"SELECT title, pages, rating FROM books WHERE pages BETWEEN 250 AND 400 AND rating > 4.0;",
    6:"SELECT title, rating FROM movies ORDER BY rating DESC LIMIT 5;",
    7:"SELECT title, genre, year FROM books WHERE genre IN ('Drame','Romance');",
    8:"SELECT title, genre, rating FROM movies WHERE genre IN ('Mystère','Thriller') AND rating >= 7.0;",
    9:"SELECT title, year FROM books WHERE year < 2010;",
    10:"SELECT title, duration_minutes, genre FROM movies WHERE duration_minutes BETWEEN 100 AND 130;",
    11:"SELECT title, year FROM books ORDER BY year DESC LIMIT 3;",
    12:"SELECT title, rating, pages FROM books WHERE rating < 4.0 OR pages < 250;",
    13:"SELECT title, year, rating FROM movies WHERE year IN (2019, 2020);",
    14:"SELECT * FROM books WHERE author_id IN (1,3,5) AND rating >= 4.0;",
    15:"SELECT * FROM movies ORDER BY genre ASC, rating DESC;",
    16:"SELECT b.title, a.name AS auteur, a.country FROM books b JOIN authors a ON a.id = b.author_id;",
    17:"SELECT m.title, d.name AS realisateur FROM movies m LEFT JOIN directors d ON d.id = m.director_id;",
    18:"SELECT a.name, COUNT(b.id) AS total_livres FROM authors a LEFT JOIN books b ON b.author_id = a.id GROUP BY a.name;",
    19:"SELECT d.name, AVG(m.rating) AS moyenne_note FROM directors d JOIN movies m ON m.director_id = d.id GROUP BY d.name;",
    20:(
        "-- SQLite ne supporte pas FULL JOIN : on le simule avec UNION + LEFT JOIN.\n"
        "WITH a AS (SELECT country, COUNT(*) AS total_auteurs FROM authors GROUP BY country),\n"
        "     d AS (SELECT country, COUNT(*) AS total_realisateurs FROM directors GROUP BY country),\n"
        "     all_c AS (SELECT country FROM a UNION SELECT country FROM d)\n"
        "SELECT all_c.country,\n"
        "       COALESCE(a.total_auteurs, 0) AS total_auteurs,\n"
        "       COALESCE(d.total_realisateurs, 0) AS total_realisateurs\n"
        "FROM all_c\n"
        "LEFT JOIN a ON a.country = all_c.country\n"
        "LEFT JOIN d ON d.country = all_c.country\n"
        "ORDER BY all_c.country;"
    )
}

HINT = {
    20: "Indice : SQLite ne prend pas en charge FULL JOIN. Utilisez une union des pays présents dans A et D, puis des LEFT JOIN de chaque côté (voir solution)."
}

# =========================
# HELPERS BD
# =========================
@st.cache_resource
def get_conn():
    c = sqlite3.connect(":memory:", check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    return c

def reset_db(c, script):
    # Nettoyage d'éventuels fragments MySQL
    script = re.sub(r"SET\s+FOREIGN_KEY_CHECKS\s*=\s*\d+;\s*", "", script, flags=re.I)
    with closing(c.cursor()) as cur:
        cur.executescript(script)
    c.commit()

def run_sql(c, q):
    with closing(c.cursor()) as cur:
        cur.execute(q)
        if re.match(r"\s*(WITH|SELECT|PRAGMA)\b", q.strip(), flags=re.I):
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols), None
        c.commit()
        return None, "OK"

# =========================
# PROGRESSION (barre + image)
# =========================
def render_bar_clickable():
    cols = st.columns(len(EXOS))
    for i, col in enumerate(cols):
        color = "#ccc"
        s = st.session_state.status[i]
        if s == "solved": color = "#2ecc71"
        elif s == "skipped": color = "#e67e22"
        with col:
            if st.button(str(i+1), key=f"barbtn_{i}_{st.session_state.render_id}", width="stretch"):
                st.session_state.step = i
            st.markdown(
                f"<div style='height:10px;background:{color};border-radius:2px;margin-top:4px;'></div>",
                unsafe_allow_html=True
            )

def save_progress_image(student_name):
    w, h = 480, 40
    sw = w // len(EXOS)
    img = Image.new("RGB", (w, h), "white")
    dr = ImageDraw.Draw(img)
    for i, s in enumerate(st.session_state.status):
        c = "#ccc"
        if s == "solved": c = "#2ecc71"
        elif s == "skipped": c = "#e67e22"
        dr.rectangle([i*sw, 0, (i+1)*sw-2, h], fill=c, outline="black")
    fname = f"{student_name}_progress.png"
    img.save(fname)
    return fname

# =========================
# UPLOAD GITHUB (robuste)
# =========================
def upload_to_github(file_path, repo, token, subdir="submissions", branch="main", message=None):
    with open(file_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("utf-8")

    filename = file_path.split("/")[-1]
    url = f"https://api.github.com/repos/{repo}/contents/{subdir}/{filename}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}

    # Vérifier existence pour SHA
    r_get = requests.get(url, headers=headers, params={"ref": branch})
    sha = r_get.json().get("sha") if r_get.status_code == 200 else None

    data = {"message": message or f"Add {filename}", "content": content_b64, "branch": branch}
    if sha:
        data["sha"] = sha

    r_put = requests.put(url, headers=headers, data=json.dumps(data))
    return r_put.status_code, r_put.text

# =========================
# ÉTAT
# =========================
if "status" not in st.session_state:
    st.session_state.status = ["locked"] * len(EXOS)   # locked | skipped | solved
    st.session_state.inputs = [""] * len(EXOS)
    st.session_state.step = 0
    st.session_state.render_id = 0

# =========================
# INIT BD
# =========================
conn = get_conn()
reset_db(conn, DEFAULT_SQL)

# =========================
# UI
# =========================
st.title("Pratique SQL — Livres & Films (Progression + Envoi GitHub)")

# Barre de progression cliquable
render_bar_clickable()
st.markdown("---")

i = st.session_state.step
st.subheader(EXOS[i])

# Zone de saisie
user_sql = st.text_area(
    "Votre requête SQL :",
    value=st.session_state.inputs[i],
    height=180,
    key=f"sql_input_{i}"
)

# Boutons
c1, c2, c3, c4 = st.columns([1,1,1,1])
with c1:
    run_btn = st.button("Exécuter", key=f"run_{i}")
with c2:
    hint_btn = st.button("Indice", key=f"hint_{i}")
with c3:
    skip_btn = st.button("Je bloque — voir la solution", key=f"skip_{i}")
with c4:
    reset_btn = st.button("Réinitialiser la base", key=f"reset_{i}")

# Indice
if hint_btn:
    msg = HINT.get(i+1, "Indice indisponible pour cet exercice.")
    st.info(msg)

# Reset DB
if reset_btn:
    reset_db(conn, DEFAULT_SQL)
    st.success("Base de données réinitialisée.")

# Skip → solution + orange persistant
if skip_btn:
    st.session_state.status[i] = "skipped"
    sol = SOL.get(i+1, "-- Pas de solution prédéfinie")
    st.code(sol, language="sql")
    # on remplit aussi la zone de réponse de l’étudiant avec la solution (trace)
    st.session_state.inputs[i] = sol
    st.session_state.render_id += 1
    st.experimental_rerun()

# Exécuter → validation
if run_btn:
    # Pas de validation vide
    if not user_sql.strip():
        st.error("Veuillez saisir une requête SQL.")
    else:
        st.session_state.inputs[i] = user_sql
        try:
            df, msg = run_sql(conn, user_sql)
            if df is not None:
                # On affiche le résultat
                st.dataframe(df, use_container_width=True)
                # Marquer comme résolu (vert)
                st.session_state.status[i] = "solved"
                # Aller à la question suivante automatiquement
                if i < len(EXOS) - 1:
                    st.session_state.step = i + 1
                st.session_state.render_id += 1
                st.experimental_rerun()
            else:
                # Pour les requêtes non-SELECT, on n'avance pas automatiquement
                st.success(msg or "OK")
        except Exception as e:
            st.error(f"Erreur SQL : {e}")

st.markdown("---")

# Soumission à tout moment
st.subheader("Soumission de votre progression")
student_name = st.text_input("Nom complet :")
submit_now = st.button("Envoyer à l’enseignant")

if submit_now:
    if not student_name.strip():
        st.error("Nom manquant.")
    else:
        safe_name = student_name.strip().replace(" ", "_")
        # Image progression
        img_path = save_progress_image(safe_name)
        # Réponses CSV
        answers_df = pd.DataFrame({
            "Exercice": EXOS,
            "Reponse": st.session_state.inputs,
            "Statut": st.session_state.status
        })
        csv_path = f"{safe_name}_answers.csv"
        answers_df.to_csv(csv_path, index=False)

        # Upload
        if TOKEN_SECRET_KEY not in st.secrets:
            st.error("Le secret GITHUB_TOKEN est introuvable dans Streamlit.")
        else:
            token = st.secrets[TOKEN_SECRET_KEY]
            code1, txt1 = upload_to_github(img_path, REPO, token, subdir=SUBDIR, branch=BRANCH, message=f"Progress {student_name}")
            code2, txt2 = upload_to_github(csv_path, REPO, token, subdir=SUBDIR, branch=BRANCH, message=f"Answers {student_name}")

            if code1 in (200,201) and code2 in (200,201):
                st.success("Fichiers envoyés dans GitHub /submissions.")
            else:
                st.error(f"Erreur d’envoi (image): {code1} {txt1}")
                st.error(f"Erreur d’envoi (CSV): {code2} {txt2}")

# Pied de page
st.caption("Note : SQLite ne supporte pas FULL JOIN. L’exercice 20 le simule via UNION + LEFT JOIN.")
