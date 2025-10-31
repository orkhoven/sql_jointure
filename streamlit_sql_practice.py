# streamlit_sql_practice_final_nav.py
import streamlit as st
import sqlite3
import pandas as pd
import re, base64, requests, json, os
from PIL import Image, ImageDraw
from contextlib import closing

st.set_page_config(page_title="Pratique SQL : Livres & Films", layout="wide")

REPO = "orkhoven/sql_panda"
BRANCH = "main"
SUBDIR = "submissions"
TOKEN_SECRET_KEY = "GITHUB_TOKEN"

# ---------- DATABASE ----------
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

# ---------- QUESTIONS ----------
QUESTIONS = [
"Listez tous les titres de livres avec leur année de publication (triés par année croissante).",
"Affichez les films dont le genre est 'Sci-Fi' (titre, année).",
"Affichez les livres dont la note est ≥ 4.5 (titre, auteur_id, note).",
"Affichez les films sortis entre 2015 et 2021 (titre, année, réalisateur_id).",
"Affichez les livres ayant entre 250 et 400 pages et une note > 4.0.",
"Affichez les 5 films les mieux notés.",
"Affichez les livres de genre 'Drame' ou 'Romance'.",
"Affichez les films de genre 'Mystère' ou 'Thriller' avec note ≥ 7.0.",
"Affichez les livres publiés avant 2010.",
"Affichez les films dont la durée est entre 100 et 130 minutes.",
"Affichez les 3 livres les plus récents.",
"Affichez les livres avec une note < 4.0 ou moins de 250 pages.",
"Affichez les films sortis en 2019 ou 2020.",
"Affichez les livres écrits par les auteurs (id 1,3,5) avec note ≥ 4.0.",
"Affichez tous les films triés par genre puis par note décroissante.",
"Listez tous les livres avec le nom et le pays de leur auteur.",
"Affichez tous les films et le nom de leur réalisateur (même si manquant).",
"Pour chaque auteur, affichez le nombre de livres écrits (y compris 0).",
"Pour chaque réalisateur, affichez la note moyenne de ses films.",
"Par pays, affichez le nombre d’auteurs et de réalisateurs."
]

# ---------- SOLUTIONS ----------
SOL = [
"SELECT title, year FROM books ORDER BY year ASC;",
"SELECT title, year FROM movies WHERE genre='Sci-Fi';",
"SELECT title, author_id, rating FROM books WHERE rating>=4.5 ORDER BY rating DESC;",
"SELECT title, year, director_id FROM movies WHERE year BETWEEN 2015 AND 2021;",
"SELECT title, pages, rating FROM books WHERE pages BETWEEN 250 AND 400 AND rating>4.0;",
"SELECT title, rating FROM movies ORDER BY rating DESC LIMIT 5;",
"SELECT title, genre, year FROM books WHERE genre IN('Drame','Romance');",
"SELECT title, genre, rating FROM movies WHERE genre IN('Mystère','Thriller') AND rating>=7.0;",
"SELECT title, year FROM books WHERE year<2010 ORDER BY year DESC;",
"SELECT title, duration_minutes, genre FROM movies WHERE duration_minutes BETWEEN 100 AND 130;",
"SELECT title, year FROM books ORDER BY year DESC LIMIT 3;",
"SELECT title, rating, pages FROM books WHERE rating<4.0 OR pages<250;",
"SELECT title, year, rating FROM movies WHERE year IN(2019,2020);",
"SELECT title FROM books WHERE author_id IN(1,3,5) AND rating>=4.0;",
"SELECT * FROM movies ORDER BY genre ASC, rating DESC;",
"SELECT b.title,a.name,a.country FROM books b JOIN authors a ON a.id=b.author_id;",
"SELECT m.title,d.name FROM movies m LEFT JOIN directors d ON d.id=m.director_id;",
"SELECT a.name,COUNT(b.id) AS total FROM authors a LEFT JOIN books b ON b.author_id=a.id GROUP BY a.name;",
"SELECT d.name,AVG(m.rating) AS moyenne FROM directors d JOIN movies m ON m.director_id=d.id GROUP BY d.name;",
"WITH a AS (SELECT country,COUNT(*) AS total_auteurs FROM authors GROUP BY country), "
"d AS (SELECT country,COUNT(*) AS total_realisateurs FROM directors GROUP BY country), "
"all_c AS (SELECT country FROM a UNION SELECT country FROM d) "
"SELECT all_c.country,COALESCE(a.total_auteurs,0) AS total_auteurs,COALESCE(d.total_realisateurs,0) AS total_realisateurs "
"FROM all_c LEFT JOIN a ON a.country=all_c.country LEFT JOIN d ON d.country=all_c.country;"
]

# ---------- HELPERS ----------
@st.cache_resource
def get_conn():
    c = sqlite3.connect(":memory:", check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    return c

def reset_db(c, s):
    with closing(c.cursor()) as cur: cur.executescript(s)
    c.commit()

def run_sql(c, q):
    with closing(c.cursor()) as cur:
        cur.execute(q)
        if re.match(r"\s*(WITH|SELECT|PRAGMA)\b", q.strip(), re.I):
            cols=[d[0] for d in cur.description] if cur.description else []
            rows=cur.fetchall()
            return pd.DataFrame(rows,columns=cols),None
        c.commit()
        return None,"OK"

def upload_git(f,repo,token,msg):
    fn=os.path.basename(f)
    url=f"https://api.github.com/repos/{repo}/contents/{SUBDIR}/{fn}"
    with open(f,"rb") as fh: content=base64.b64encode(fh.read()).decode("utf-8")
    h={"Authorization":f"token {token}"}
    r=requests.get(url,headers=h)
    sha=r.json().get("sha") if r.status_code==200 else None
    data={"message":msg,"content":content,"branch":BRANCH}
    if sha:data["sha"]=sha
    p=requests.put(url,headers=h,data=json.dumps(data))
    return p.status_code

# ---------- SESSION ----------
if "status" not in st.session_state:
    st.session_state.status=["locked"]*len(QUESTIONS)
    st.session_state.inputs=[""]*len(QUESTIONS)
    st.session_state.step=0

conn=get_conn();reset_db(conn,DEFAULT_SQL)

# ---------- UI ----------
st.title("Pratique SQL — Livres & Films")

cols = st.columns(len(QUESTIONS))
for i, col in enumerate(cols):
    color = "#ccc"
    if st.session_state.status[i] == "solved": color = "#2ecc71"
    elif st.session_state.status[i] == "skipped": color = "#e67e22"
    with col:
        if st.button(str(i+1), key=f"q{i}", use_container_width=True):
            st.session_state.step = i
        st.markdown(
            f"<div style='background:{color};height:5px;border-radius:2px'></div>",
            unsafe_allow_html=True
        )

i = st.session_state.step
st.subheader(f"Exercice {i+1}")
st.write(QUESTIONS[i])

user=st.text_area("Votre requête SQL :", st.session_state.inputs[i], height=150)

c1,c2,c3=st.columns(3)
run=c1.button("Exécuter")
see_sol=c2.button("Voir la solution")
reset=c3.button("Réinitialiser la base")

if reset:
    reset_db(conn,DEFAULT_SQL)
    st.success("Base réinitialisée.")

if see_sol:
    sol_text = SOL[i]
    st.session_state.status[i] = "skipped"
    st.code(sol_text, language="sql")

if run:
    q=user.strip()
    if not q:
        st.error("Veuillez saisir une requête SQL.")
    else:
        st.session_state.inputs[i]=q
        try:
            df,msg=run_sql(conn,q)
            if df is not None:
                if df.empty:
                    st.warning("Résultat vide.")
                else:
                    st.dataframe(df,use_container_width=True)
                    st.session_state.status[i]="solved"
            elif msg and msg!="OK":
                st.success(msg)
            else:
                st.warning("Aucune donnée retournée.")
        except Exception as e:
            st.error(f"Erreur SQL : {e}")

st.markdown("---")
st.subheader("Soumission de votre progression")

name=st.text_input("Nom complet :")
if st.button("Envoyer à l’enseignant"):
    if not name.strip():
        st.error("Nom manquant.")
    else:
        df=pd.DataFrame({
            "Exercice":list(range(1,len(QUESTIONS)+1)),
            "Question":QUESTIONS,
            "Réponse":st.session_state.inputs,
            "Statut":st.session_state.status
        })
        csv=f"{name.replace(' ','_')}_answers.csv"
        df.to_csv(csv,index=False)
        if TOKEN_SECRET_KEY not in st.secrets:
            st.error("Secret GITHUB_TOKEN introuvable.")
        else:
            token=st.secrets[TOKEN_SECRET_KEY]
            code=upload_git(csv,REPO,token,f"Answers {name}")
            if code in [200,201]:
                st.success("Fichier CSV envoyé avec succès vers GitHub /submissions.")
            else:
                st.error(f"Erreur d’envoi : code {code}")
