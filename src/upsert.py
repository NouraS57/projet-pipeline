"""UPSERT PostgreSQL : INSERT ... ON CONFLICT DO UPDATE."""
import csv
import logging
import os

import cyber_pipeline as cp
from sql_ingestion import CLEAN_DIR, connexion

log = logging.getLogger(__name__)


def preparer_table():
    with connexion() as conn, conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS evenements (
                   cle TEXT PRIMARY KEY,
                   horodatage TEXT,
                   mois TEXT,
                   source_ip TEXT,
                   destination_ip TEXT,
                   attack_type TEXT,
                   severity_level TEXT,
                   anomaly_score NUMERIC,
                   action_taken TEXT,
                   user_hash TEXT,
                   maj_le TIMESTAMP DEFAULT now())"""
        )


def upsert_mois(mois, modifier_score=False):
    """Charge attacks_<mois>.csv dans la table 'evenements' avec UPSERT."""
    preparer_table()
    chemin = os.path.join(CLEAN_DIR, f"attacks_{mois}.csv")
    if not os.path.exists(chemin):
        cp.load(cp.transform(cp.validate(cp.extract(mois))))

    lignes, nb_fichier = {}, 0
    with open(chemin, newline="", encoding="utf-8") as f:
        for l in csv.DictReader(f):
            nb_fichier += 1
            score = float(l["anomaly_score"])
            if modifier_score:
                score = round(score + 1, 2)
            cle = "|".join([l["timestamp"], l["source_ip"], l["destination_ip"],
                            l["source_port"], l["destination_port"]])
            lignes[cle] = (cle, l["timestamp"], l["mois"], l["source_ip"], l["destination_ip"],
                           l["attack_type"], l["severity_level"], score,
                           l["action_taken"], l["user_hash"])

    inseres = misajour = 0
    with connexion() as conn, conn.cursor() as cur:
        for ligne in lignes.values():
            cur.execute(
                """INSERT INTO evenements (cle, horodatage, mois, source_ip, destination_ip,
                       attack_type, severity_level, anomaly_score, action_taken, user_hash)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (cle) DO UPDATE SET
                       anomaly_score = EXCLUDED.anomaly_score,
                       severity_level = EXCLUDED.severity_level,
                       action_taken = EXCLUDED.action_taken,
                       maj_le = now()
                   RETURNING (xmax = 0)""",
                ligne,
            )
            if cur.fetchone()[0]:
                inseres += 1
            else:
                misajour += 1
        cur.execute("SELECT COUNT(*) FROM evenements WHERE mois = %s", (mois,))
        total = cur.fetchone()[0]

    log.info("UPSERT %s : fichier=%s lignes, cles uniques=%s, inserees=%s, mises a jour=%s, total table=%s",
             mois, nb_fichier, len(lignes), inseres, misajour, total)
    return {"mois": mois, "inseres": inseres, "mises_a_jour": misajour, "total_table": total}