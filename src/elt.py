"""ELT : charger le brut dans PostgreSQL, puis transformer en SQL."""
import csv
import logging
import os
import time

import cyber_pipeline as cp
from sql_ingestion import CLEAN_DIR, connexion

log = logging.getLogger(__name__)


def charger_brut(mois):
    """E + L : copie les lignes brutes du mois dans PostgreSQL, sans transformation."""
    debut = time.time()
    lignes = []
    with open(cp.RAW_FILE, newline="", encoding="utf-8") as f:
        for l in csv.DictReader(f):
            if l["Timestamp"].startswith(mois):
                lignes.append((mois, l["Timestamp"], l["Source IP Address"], l["Attack Type"],
                               l["Severity Level"], l["Anomaly Scores"], l["Malware Indicators"]))
    with connexion() as conn, conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS brut_evenements (
                   mois TEXT, horodatage TEXT, source_ip TEXT, attack_type TEXT,
                   severity_level TEXT, anomaly_score TEXT, malware TEXT)"""
        )
        cur.execute("DELETE FROM brut_evenements WHERE mois = %s", (mois,))
        cur.executemany("INSERT INTO brut_evenements VALUES (%s,%s,%s,%s,%s,%s,%s)", lignes)
    log.info("ELT - chargement brut %s : %s lignes copiees sans transformation (%.1fs)",
             mois, len(lignes), time.time() - debut)
    return {"mois": mois, "lignes_brutes": len(lignes)}


def transformer_sql(info):
    """T : la transformation est faite par PostgreSQL, avec une requete SQL."""
    mois = info["mois"]
    debut = time.time()
    with connexion() as conn, conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS elt_resume_gravite (
                   mois TEXT, severity_level TEXT, nb_evenements INT,
                   score_moyen NUMERIC(6,2), nb_malware INT,
                   PRIMARY KEY (mois, severity_level))"""
        )
        cur.execute("DELETE FROM elt_resume_gravite WHERE mois = %s", (mois,))
        cur.execute(
            """INSERT INTO elt_resume_gravite
               SELECT mois, severity_level, COUNT(*),
                      ROUND(AVG(anomaly_score::numeric), 2),
                      COUNT(*) FILTER (WHERE malware <> '')
               FROM brut_evenements WHERE mois = %s
               GROUP BY mois, severity_level""",
            (mois,),
        )
        cur.execute("SELECT severity_level, nb_evenements, score_moyen, nb_malware "
                    "FROM elt_resume_gravite WHERE mois = %s ORDER BY severity_level", (mois,))
        resultat = cur.fetchall()
    for niveau, nb, score, malware in resultat:
        log.info("ELT - SQL %s : %s evenements, score moyen %s, %s avec malware", niveau, nb, score, malware)
    log.info("ELT - transformation SQL terminee (%.2fs)", time.time() - debut)
    return {"mois": mois, "elt": {r[0]: r[1] for r in resultat}}


def comparer_etl(info):
    """Compare avec le resultat ETL (transformation Python deja faite dans le pipeline)."""
    mois = info["mois"]
    chemin = os.path.join(CLEAN_DIR, f"attacks_{mois}.csv")
    if not os.path.exists(chemin):
        cp.load(cp.transform(cp.validate(cp.extract(mois))))
    etl = {}
    with open(chemin, newline="", encoding="utf-8") as f:
        for l in csv.DictReader(f):
            etl[l["severity_level"]] = etl.get(l["severity_level"], 0) + 1
    log.info("COMPARAISON %s | ETL (Python, apres validation) : %s | total=%s",
             mois, dict(sorted(etl.items())), sum(etl.values()))
    log.info("COMPARAISON %s | ELT (SQL, brut non filtre)     : %s | total=%s",
             mois, dict(sorted(info["elt"].items())), sum(info["elt"].values()))
    return {"mois": mois, "etl": etl, "elt": info["elt"]}