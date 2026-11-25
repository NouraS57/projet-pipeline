"""Ingestion SQL : lecture d'une table de reference PostgreSQL dans le pipeline."""
import csv
import logging
import os

import psycopg2

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_FILE = os.path.join(DATA_DIR, "raw", "cybersecurity_attacks.csv")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")

DB = dict(
    host=os.environ.get("PG_HOST", "postgres"),
    port=5432,
    dbname="projet",
    user="airflow",
    password="airflow",
)

REFERENTIEL = [
    ("High", 1, 1),
    ("Medium", 2, 24),
    ("Low", 3, 72),
]


def connexion():
    return psycopg2.connect(**DB)


def preparer_referentiel():
    """Cree la table de reference si besoin et la remplit (relancable sans doublon)."""
    with connexion() as conn, conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS referentiel_severite (
                   niveau TEXT PRIMARY KEY,
                   priorite INT NOT NULL,
                   delai_reaction_h INT NOT NULL)"""
        )
        cur.executemany(
            """INSERT INTO referentiel_severite (niveau, priorite, delai_reaction_h)
               VALUES (%s, %s, %s) ON CONFLICT (niveau) DO NOTHING""",
            REFERENTIEL,
        )
        cur.execute("SELECT COUNT(*) FROM referentiel_severite")
        nb = cur.fetchone()[0]
    log.info("table referentiel_severite prete : %s lignes", nb)
    return {"table": "referentiel_severite", "nb_lignes": nb}


def lire_referentiel():
    """Lit la table en SQL."""
    with connexion() as conn, conn.cursor() as cur:
        cur.execute("SELECT niveau, priorite, delai_reaction_h FROM referentiel_severite ORDER BY priorite")
        lignes = cur.fetchall()
    log.info("ingestion SQL : %s lignes lues dans referentiel_severite", len(lignes))
    return lignes


def enrichir_par_severite(info, mois):
    """Compte les evenements du mois par gravite et ajoute les infos lues en SQL."""
    ref = {niveau: (priorite, delai) for niveau, priorite, delai in lire_referentiel()}
    compteurs = {}
    with open(RAW_FILE, newline="", encoding="utf-8") as f:
        for l in csv.DictReader(f):
            if l["Timestamp"].startswith(mois):
                compteurs[l["Severity Level"]] = compteurs.get(l["Severity Level"], 0) + 1
    os.makedirs(CLEAN_DIR, exist_ok=True)
    sortie = os.path.join(CLEAN_DIR, f"rapport_severite_{mois}.csv")
    with open(sortie, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["niveau", "nb_evenements", "priorite", "delai_reaction_h"])
        for niveau, (priorite, delai) in sorted(ref.items(), key=lambda x: x[1][0]):
            w.writerow([niveau, compteurs.get(niveau, 0), priorite, delai])
            log.info("%s : %s evenements, priorite %s, delai %s h",
                     niveau, compteurs.get(niveau, 0), priorite, delai)
    return {"mois": mois, "path": sortie, "total": sum(compteurs.values())}