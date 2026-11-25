"""Decouplage : un fichier CSV et un fichier plat, traites par deux branches independantes."""
import csv
import logging
import os

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_FILE = os.path.join(DATA_DIR, "raw", "cybersecurity_attacks.csv")
STAGING_DIR = os.path.join(DATA_DIR, "staging")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")

# Fichier plat a largeurs fixes : (nom du champ, largeur en caracteres)
CHAMPS_PLAT = [("timestamp", 19), ("source_ip", 15), ("attack_type", 10),
               ("severity", 6), ("score", 6)]


def generer_fichier_plat(mois):
    """Simule un systeme externe qui depose un fichier plat des alertes critiques (High)."""
    chemin = os.path.join(DATA_DIR, "raw", f"alertes_{mois}.dat")
    n = 0
    with open(RAW_FILE, newline="", encoding="utf-8") as f, \
            open(chemin + ".tmp", "w", encoding="utf-8") as sortie:
        for l in csv.DictReader(f):
            if l["Timestamp"].startswith(mois) and l["Severity Level"] == "High":
                valeurs = [l["Timestamp"], l["Source IP Address"], l["Attack Type"],
                           l["Severity Level"], f"{float(l['Anomaly Scores']):.2f}"]
                sortie.write("".join(str(v)[:w].ljust(w)
                                     for v, (_, w) in zip(valeurs, CHAMPS_PLAT)) + "\n")
                n += 1
    os.replace(chemin + ".tmp", chemin)
    log.info("fichier plat depose : %s (%s alertes critiques)", chemin, n)
    return chemin


def lire_fichier_plat(chemin, mois):
    """Lit le fichier plat (decoupage par largeurs fixes) et l'ecrit en CSV de staging."""
    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Fichier plat introuvable : {chemin}")
    lignes = []
    with open(chemin, encoding="utf-8") as f:
        for texte in f:
            texte = texte.rstrip("\n")
            debut, ligne = 0, {}
            for nom, largeur in CHAMPS_PLAT:
                ligne[nom] = texte[debut:debut + largeur].strip()
                debut += largeur
            lignes.append(ligne)
    sortie = os.path.join(STAGING_DIR, "plat", mois, "alertes.csv")
    os.makedirs(os.path.dirname(sortie), exist_ok=True)
    with open(sortie, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[nom for nom, _ in CHAMPS_PLAT])
        w.writeheader()
        w.writerows(lignes)
    log.info("fichier plat lu : %s lignes -> %s", len(lignes), sortie)
    return {"mois": mois, "path": sortie, "nb_lignes": len(lignes)}


def fusionner(info_csv, info_plat):
    """Croise les deux sources : combien d'alertes du fichier plat existent dans le CSV ?"""
    with open(info_csv["path"], newline="", encoding="utf-8") as f:
        cles_csv = {(l["Timestamp"], l["Source IP Address"]) for l in csv.DictReader(f)}
    with open(info_plat["path"], newline="", encoding="utf-8") as f:
        cles_plat = [(l["timestamp"], l["source_ip"]) for l in csv.DictReader(f)]
    croisees = sum(1 for c in cles_plat if c in cles_csv)
    resultat = {"mois": info_csv["mois"], "lignes_csv": len(cles_csv),
                "alertes_fichier_plat": len(cles_plat), "alertes_retrouvees": croisees}
    chemin = os.path.join(CLEAN_DIR, f"croisement_{info_csv['mois']}.csv")
    os.makedirs(CLEAN_DIR, exist_ok=True)
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(resultat))
        w.writeheader()
        w.writerow(resultat)
    log.info("fusion : %s lignes CSV, %s alertes du fichier plat, %s retrouvees -> %s",
             len(cles_csv), len(cles_plat), croisees, chemin)
    return resultat