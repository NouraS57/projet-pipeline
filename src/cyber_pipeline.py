"""Fonctions du pipeline cybersecurite : extract, validate, transform, load."""
import csv
import hashlib
import ipaddress
import logging
import os
from collections import defaultdict
from datetime import datetime

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_FILE = os.path.join(DATA_DIR, "raw", "cybersecurity_attacks.csv")
STAGING_DIR = os.path.join(DATA_DIR, "staging")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
REJECTED_DIR = os.path.join(DATA_DIR, "rejected")

# --- Contrat de donnees -------------------------------------------------
COLONNES = [
    "Timestamp", "Source IP Address", "Destination IP Address", "Source Port",
    "Destination Port", "Protocol", "Packet Length", "Packet Type", "Traffic Type",
    "Payload Data", "Malware Indicators", "Anomaly Scores", "Alerts/Warnings",
    "Attack Type", "Attack Signature", "Action Taken", "Severity Level",
    "User Information", "Device Information", "Network Segment",
    "Geo-location Data", "Proxy Information", "Firewall Logs", "IDS/IPS Alerts",
    "Log Source",
]
VALEURS_PERMISES = {
    "Protocol": {"TCP", "UDP", "ICMP"},
    "Packet Type": {"Data", "Control"},
    "Traffic Type": {"HTTP", "DNS", "FTP"},
    "Attack Type": {"Malware", "Intrusion", "DDoS"},
    "Action Taken": {"Blocked", "Logged", "Ignored"},
    "Severity Level": {"Low", "Medium", "High"},
}
SEUIL_REJET = 0.10  # au-dela de 10 % de lignes rejetees, la tache echoue
FLAGS = {
    "Malware Indicators": "has_malware",
    "Alerts/Warnings": "has_alert",
    "Proxy Information": "has_proxy",
    "Firewall Logs": "has_firewall_log",
    "IDS/IPS Alerts": "has_ids_alert",
}


def _ecrire_csv(chemin, colonnes, lignes):
    """Ecriture atomique : fichier temporaire puis remplacement (idempotent)."""
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    tmp = chemin + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=colonnes, extrasaction="ignore")
        w.writeheader()
        w.writerows(lignes)
    os.replace(tmp, chemin)


def _lire_csv(chemin):
    with open(chemin, newline="", encoding="utf-8") as f:
        lecteur = csv.DictReader(f)
        return list(lecteur.fieldnames), list(lecteur)


# --- 0. periode a traiter -------------------------------------------------
def choisir_mois(params=None, data_interval_start=None):
    """Retourne le mois a traiter, au format 'AAAA-MM'.

    Priorite : le parametre 'mois' choisi a la main, sinon le debut de la
    periode planifiee (cas du backfill et des executions automatiques).
    """
    mois = ((params or {}).get("mois") or "").strip()
    if not mois and data_interval_start is not None:
        mois = data_interval_start.strftime("%Y-%m")
    if not mois:
        raise ValueError("Aucun mois a traiter : renseigner le parametre 'mois' (ex. 2022-03)")
    datetime.strptime(mois, "%Y-%m")  # leve une erreur si le format est faux
    log.info("periode : mois a traiter = %s", mois)
    return mois


# --- 1. extract -----------------------------------------------------------
def extract(mois=None):
    """Lit le CSV brut (tout, ou un seul mois 'YYYY-MM') et l'ecrit en staging.

    Chaque mois a son propre dossier de staging : deux mois peuvent donc
    etre traites en meme temps sans se marcher dessus.
    """
    etiquette = mois or "tous"
    log.info("extract : debut (mois=%s)", etiquette)
    colonnes, lignes = _lire_csv(RAW_FILE)
    if mois:
        lignes = [l for l in lignes if l["Timestamp"].startswith(mois)]
    chemin = os.path.join(STAGING_DIR, etiquette, "extracted.csv")
    _ecrire_csv(chemin, colonnes, lignes)
    log.info("extract : %s lignes ecrites dans %s", len(lignes), chemin)
    return {"mois": etiquette, "path": chemin, "nb_lignes": len(lignes)}


# --- 2. validate ------------------------------------------------------------
def _verifier_ligne(l):
    """Retourne le motif de rejet, ou None si la ligne respecte le contrat."""
    try:
        datetime.strptime(l["Timestamp"], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return "Timestamp invalide"
    for c in ("Source IP Address", "Destination IP Address"):
        try:
            ipaddress.ip_address(l[c])
        except ValueError:
            return f"{c} invalide"
    for c, mini, maxi in (("Source Port", 0, 65535), ("Destination Port", 0, 65535),
                          ("Packet Length", 64, 1500), ("Anomaly Scores", 0, 100)):
        try:
            v = float(l[c])
        except ValueError:
            return f"{c} non numerique"
        if not mini <= v <= maxi:
            return f"{c} hors limites"
    for c, permises in VALEURS_PERMISES.items():
        if l[c] not in permises:
            return f"{c} valeur inconnue"
    return None


def validate(info):
    """Applique le contrat : schema strict, puis controle ligne par ligne."""
    colonnes, lignes = _lire_csv(info["path"])
    if colonnes != COLONNES:
        manquantes = sorted(set(COLONNES) - set(colonnes))
        raise ValueError(f"Contrat viole : colonnes manquantes ou differentes {manquantes}")
    valides, rejetees = [], []
    for l in lignes:
        motif = _verifier_ligne(l)
        if motif:
            rejetees.append({**l, "motif": motif})
        else:
            valides.append(l)
    chemin_ok = os.path.join(os.path.dirname(info["path"]), "valid.csv")
    chemin_ko = os.path.join(REJECTED_DIR, f"rejected_{info['mois']}.csv")
    _ecrire_csv(chemin_ok, COLONNES, valides)
    _ecrire_csv(chemin_ko, COLONNES + ["motif"], rejetees)
    log.info("validate : %s valides, %s rejetees", len(valides), len(rejetees))
    if lignes and len(rejetees) / len(lignes) > SEUIL_REJET:
        raise ValueError(f"Trop de lignes rejetees : {len(rejetees)}/{len(lignes)}")
    return {"mois": info["mois"], "path": chemin_ok, "valides": len(valides), "rejetees": len(rejetees)}


# --- 3. transform -----------------------------------------------------------
SEL_GRAVITE = {"Low": 1, "Medium": 2, "High": 3}
SEL_HASH = "dataops-2026"  # sel ajoute avant le hachage des noms


COLONNES_SORTIE = [
    "timestamp", "date", "mois", "heure", "jour_semaine", "source_ip",
    "destination_ip", "source_port", "destination_port", "protocol",
    "packet_length", "packet_type", "traffic_type", "payload_data",
    "anomaly_score", "attack_type", "attack_signature", "action_taken",
    "severity_level", "severity_score", "user_hash", "device_info",
    "network_segment", "ville", "etat", "proxy_ip", "log_source",
    "has_malware", "has_alert", "has_proxy", "has_firewall_log",
    "has_ids_alert", "nb_indicateurs",
]


def _pseudonymiser(nom):
    """Remplace un nom par une empreinte : on peut compter par personne sans la nommer."""
    return hashlib.sha256((SEL_HASH + nom).encode("utf-8")).hexdigest()[:12]


def _transformer_ligne(l):
    """Transforme une ligne valide vers le schema propre (noms en snake_case)."""
    dt = datetime.strptime(l["Timestamp"], "%Y-%m-%d %H:%M:%S")
    ville, etat = [x.strip() for x in l["Geo-location Data"].split(",", 1)]
    flags = {flag: 1 if l[src] else 0 for src, flag in FLAGS.items()}
    return {
        "timestamp": l["Timestamp"],
        "date": dt.strftime("%Y-%m-%d"),
        "mois": dt.strftime("%Y-%m"),
        "heure": dt.hour,
        "jour_semaine": dt.isoweekday(),  # 1 = lundi ... 7 = dimanche
        "source_ip": l["Source IP Address"],
        "destination_ip": l["Destination IP Address"],
        "source_port": int(l["Source Port"]),
        "destination_port": int(l["Destination Port"]),
        "protocol": l["Protocol"],
        "packet_length": int(l["Packet Length"]),
        "packet_type": l["Packet Type"],
        "traffic_type": l["Traffic Type"],
        "payload_data": l["Payload Data"],
        "anomaly_score": round(float(l["Anomaly Scores"]), 2),
        "attack_type": l["Attack Type"],
        "attack_signature": l["Attack Signature"],
        "action_taken": l["Action Taken"],
        "severity_level": l["Severity Level"],
        "severity_score": SEL_GRAVITE[l["Severity Level"]],
        "user_hash": _pseudonymiser(l["User Information"]),
        "device_info": l["Device Information"],
        "network_segment": l["Network Segment"],
        "ville": ville,
        "etat": etat,
        "proxy_ip": l["Proxy Information"],
        "log_source": l["Log Source"],
        **flags,
        "nb_indicateurs": sum(flags.values()),
    }


def transform(info):
    """Nettoie, dedoublonne, convertit les types et construit le schema propre."""
    colonnes, lignes = _lire_csv(info["path"])
    vus, sortie = set(), []
    for l in lignes:
        l = {k: (v or "").strip() for k, v in l.items()}
        cle = (l["Timestamp"], l["Source IP Address"], l["Destination IP Address"])
        if cle in vus:
            continue
        vus.add(cle)
        sortie.append(_transformer_ligne(l))
    chemin = os.path.join(os.path.dirname(info["path"]), "transformed.csv")
    _ecrire_csv(chemin, COLONNES_SORTIE, sortie)
    log.info("transform : %s lignes (%s doublons retires), %s colonnes en sortie",
             len(sortie), len(lignes) - len(sortie), len(COLONNES_SORTIE))
    return {"mois": info["mois"], "path": chemin, "nb_lignes": len(sortie),
            "nb_colonnes": len(COLONNES_SORTIE)}


# --- 4. load ----------------------------------------------------------------
def load(info):
    """Ecrit un fichier par mois dans data/clean. Relancer = remplacer, pas ajouter."""
    colonnes, lignes = _lire_csv(info["path"])
    par_mois = defaultdict(list)
    for l in lignes:
        par_mois[l["mois"]].append(l)
    for mois, groupe in sorted(par_mois.items()):
        chemin = os.path.join(CLEAN_DIR, f"attacks_{mois}.csv")
        _ecrire_csv(chemin, colonnes, groupe)
        log.info("load : %s -> %s lignes", chemin, len(groupe))
    if not par_mois:
        log.warning("load : aucune ligne pour %s, rien a ecrire", info["mois"])
    log.info("load : %s mois ecrits, %s lignes au total", len(par_mois), len(lignes))
    return {m: len(g) for m, g in sorted(par_mois.items())}