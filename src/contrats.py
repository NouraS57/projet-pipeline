"""Contrats entre les taches du pipeline et resume d'execution (logging)."""
import logging
import os
import time

log = logging.getLogger(__name__)

# Ce que validate doit transmettre a transform
CONTRAT_VALIDATE_VERS_TRANSFORM = {"mois": str, "path": str, "valides": int, "rejetees": int}
# Ce que transform doit transmettre a load
CONTRAT_TRANSFORM_VERS_LOAD = {"mois": str, "path": str, "nb_lignes": int, "nb_colonnes": int}


class ContratViole(Exception):
    """Levee quand ce qu'une tache recoit ne respecte pas le contrat."""


def verifier_contrat(info, contrat, nom):
    """Verifie les champs, leurs types et la presence du fichier transmis."""
    if not isinstance(info, dict):
        raise ContratViole(f"Contrat '{nom}' viole : un dictionnaire etait attendu")
    manquants = [c for c in contrat if c not in info]
    if manquants:
        raise ContratViole(f"Contrat '{nom}' viole : champs manquants {manquants}")
    for champ, type_attendu in contrat.items():
        if not isinstance(info[champ], type_attendu):
            raise ContratViole(
                f"Contrat '{nom}' viole : champ '{champ}' de type "
                f"{type_attendu.__name__} attendu, {type(info[champ]).__name__} recu"
            )
    if not os.path.exists(info["path"]):
        raise ContratViole(f"Contrat '{nom}' viole : fichier introuvable {info['path']}")
    log.info("CONTRAT OK | %s | champs verifies : %s", nom, list(contrat))


def resume(etape, debut, **chiffres):
    """Ecrit une ligne de resume pour une etape terminee."""
    duree = round(time.time() - debut, 2)
    details = " | ".join(f"{k}={v}" for k, v in chiffres.items())
    log.info("RESUME | etape=%s | statut=OK | duree=%ss | %s", etape, duree, details)