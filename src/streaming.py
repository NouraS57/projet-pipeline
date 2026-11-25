"""Streaming simule : les evenements arrivent par petits paquets et sont traites au fil de l'eau."""
import csv
import logging
import os
import time

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_FILE = os.path.join(DATA_DIR, "raw", "cybersecurity_attacks.csv")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")

SEUIL_SCORE = 90.0  # une alerte est levee si gravite High et score d'anomalie >= 90


def flux_evenements(mois, limite):
    """Produit les evenements du mois dans leur ordre d'arrivee (par horodatage)."""
    with open(RAW_FILE, newline="", encoding="utf-8") as f:
        evenements = [l for l in csv.DictReader(f) if l["Timestamp"].startswith(mois)]
    evenements.sort(key=lambda l: l["Timestamp"])
    return evenements[:limite]


def est_alerte(evenement):
    return (evenement["Severity Level"] == "High"
            and float(evenement["Anomaly Scores"]) >= SEUIL_SCORE)


def lancer_streaming(mois, nb_evenements=300, taille_batch=25, pause=0.5):
    """Traite le flux par micro-batchs et leve une alerte des qu'un evenement est dangereux."""
    evenements = flux_evenements(mois, nb_evenements)
    debut = time.time()
    alertes, premiere_alerte_s = [], None
    for numero, i in enumerate(range(0, len(evenements), taille_batch), start=1):
        micro_batch = evenements[i:i + taille_batch]
        nouvelles = [e for e in micro_batch if est_alerte(e)]
        for e in nouvelles:
            log.warning("ALERTE | %s | %s | %s | score=%s", e["Timestamp"],
                        e["Source IP Address"], e["Attack Type"], e["Anomaly Scores"])
        if nouvelles and premiere_alerte_s is None:
            premiere_alerte_s = round(time.time() - debut, 2)
        alertes += nouvelles
        log.info("BATCH %s | evenements=%s | cumul=%s | alertes_batch=%s | alertes_total=%s",
                 numero, len(micro_batch), min(i + taille_batch, len(evenements)),
                 len(nouvelles), len(alertes))
        time.sleep(pause)  # simule l'attente des prochains evenements
    duree = round(time.time() - debut, 2)

    chemin = os.path.join(CLEAN_DIR, f"streaming_alertes_{mois}.csv")
    os.makedirs(CLEAN_DIR, exist_ok=True)
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "source_ip", "attack_type", "anomaly_score"])
        for e in alertes:
            w.writerow([e["Timestamp"], e["Source IP Address"], e["Attack Type"], e["Anomaly Scores"]])
    log.info("STREAMING termine : %s evenements, %s alertes, duree=%ss -> %s",
             len(evenements), len(alertes), duree, chemin)
    return {"evenements": len(evenements), "alertes": len(alertes),
            "premiere_alerte_s": premiere_alerte_s, "duree_totale_s": duree}