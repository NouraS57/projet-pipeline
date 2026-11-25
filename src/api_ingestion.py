"""Ingestion API (simulee) : un petit serveur local fournit une liste d'IP malveillantes."""
import csv
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_FILE = os.path.join(DATA_DIR, "raw", "cybersecurity_attacks.csv")
SORTIE_API = os.path.join(DATA_DIR, "raw", "api_ips_malveillantes.csv")

HOTE, PORT = "127.0.0.1", 8008
TOKEN_VALIDE = "token-secret-dataops"
TAILLE_PAGE = 50
SEUIL_SCORE = 95.0


def construire_liste_noire():
    """Prepare les donnees que l'API va servir : IP source avec un score d'anomalie eleve."""
    meilleurs = {}
    with open(RAW_FILE, newline="", encoding="utf-8") as f:
        for l in csv.DictReader(f):
            score = float(l["Anomaly Scores"])
            if l["Severity Level"] == "High" and score >= SEUIL_SCORE:
                ip = l["Source IP Address"]
                meilleurs[ip] = max(meilleurs.get(ip, 0), score)
    return [{"ip": ip, "score_max": s} for ip, s in sorted(meilleurs.items())]


def _creer_serveur(donnees, instable):
    """Cree le serveur HTTP de l'API simulee (authentification, pagination, panne au 1er appel)."""
    etat = {"appels": 0}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # pas de bruit dans les journaux
            pass

        def _repondre(self, code, corps):
            octets = json.dumps(corps).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(octets)))
            self.end_headers()
            self.wfile.write(octets)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path != "/ips-malveillantes":
                return self._repondre(404, {"erreur": "route inconnue"})
            if self.headers.get("Authorization") != f"Bearer {TOKEN_VALIDE}":
                return self._repondre(401, {"erreur": "token invalide"})
            etat["appels"] += 1
            if instable and etat["appels"] == 1:
                return self._repondre(503, {"erreur": "service temporairement indisponible"})
            page = int(parse_qs(url.query).get("page", ["1"])[0])
            debut = (page - 1) * TAILLE_PAGE
            self._repondre(200, {
                "page": page, "total": len(donnees),
                "page_suivante": page + 1 if debut + TAILLE_PAGE < len(donnees) else None,
                "resultats": donnees[debut:debut + TAILLE_PAGE],
            })

    return ThreadingHTTPServer((HOTE, PORT), Handler)


def appeler_api(url, token, tentatives=3, attente=1):
    """Appelle l'API. Erreurs temporaires (429, 503) : on reessaie. Autres erreurs : on echoue."""
    for essai in range(1, tentatives + 1):
        requete = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(requete, timeout=5) as reponse:
                return json.load(reponse)
        except urllib.error.HTTPError as erreur:
            if erreur.code in (429, 503) and essai < tentatives:
                log.warning("API : HTTP %s (essai %s/%s), nouvelle tentative dans %ss",
                            erreur.code, essai, tentatives, attente * essai)
                time.sleep(attente * essai)
                continue
            raise RuntimeError(f"Appel API refuse : HTTP {erreur.code} ({erreur.reason})")


def ingerer_api(token=TOKEN_VALIDE, instable=True):
    """Demarre l'API simulee, recupere toutes les pages et ecrit le resultat en CSV."""
    donnees = construire_liste_noire()
    serveur = _creer_serveur(donnees, instable)
    fil = threading.Thread(target=serveur.serve_forever, daemon=True)
    fil.start()
    log.info("API simulee demarree sur http://%s:%s (%s IP a servir)", HOTE, PORT, len(donnees))
    recuperees, page = [], 1
    try:
        while page:
            reponse = appeler_api(f"http://{HOTE}:{PORT}/ips-malveillantes?page={page}", token)
            recuperees += reponse["resultats"]
            log.info("API : page %s recue (%s IP), total=%s/%s", page,
                     len(reponse["resultats"]), len(recuperees), reponse["total"])
            page = reponse["page_suivante"]
    finally:
        serveur.shutdown()
        serveur.server_close()
    os.makedirs(os.path.dirname(SORTIE_API), exist_ok=True)
    with open(SORTIE_API, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ip", "score_max"])
        w.writeheader()
        w.writerows(recuperees)
    log.info("ingestion API terminee : %s IP -> %s", len(recuperees), SORTIE_API)
    return {"path": SORTIE_API, "nb_ip": len(recuperees)}


def enrichir_evenements(info, mois):
    """Croise les evenements du mois avec la liste noire recuperee par l'API."""
    with open(info["path"], newline="", encoding="utf-8") as f:
        liste_noire = {l["ip"] for l in csv.DictReader(f)}
    total = correspondances = 0
    with open(RAW_FILE, newline="", encoding="utf-8") as f:
        for l in csv.DictReader(f):
            if l["Timestamp"].startswith(mois):
                total += 1
                correspondances += l["Source IP Address"] in liste_noire
    log.info("enrichissement %s : %s evenements, %s viennent d'une IP de la liste noire",
             mois, total, correspondances)
    return {"mois": mois, "evenements": total, "ip_liste_noire": correspondances}