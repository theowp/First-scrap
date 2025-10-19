"""
Scraper des Notices Rouges Interpol (HTML uniquement avec BeautifulSoup)

Objectif
- Récupérer des pages de notices (HTML) et extraire:
  - Éléments d'identification
  - Qualification de l'infraction (ou des infractions)

Contraintes
- Utiliser uniquement BeautifulSoup pour le parsing (pas d'API JSON).
- Code simple (niveau débutant), robuste aux variations mineures.

Usage
  python main.py --urls-file urls.txt --output notices.csv

Le fichier urls.txt doit contenir une URL de notice par ligne (pages détail).
Exemple d'URL (à adapter selon la langue/site):
  https://www.interpol.int/fr/Comment-nous-fonctionnons/Notices/Consulter-les-notices-rouges/XXXX

Notes
- Le site d'Interpol peut bloquer certaines IPs. En cas de page de "failover" ou 
  d'accès refusé, fournissez une liste d'URLs de notices valides dans --urls-file
  (ou utilisez un proxy au niveau système) et relancez.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


# --------------------------- Configuration ---------------------------

DEFAULT_DELAY_SECONDS = 0.8
REQUEST_TIMEOUT_SECONDS = 25
DEFAULT_OUTPUT_CSV = "interpol_red_notices.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    # Forcer le français pour augmenter les chances de trouver
    # les intitulés 'Éléments d'identification' et 'Qualification...'
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.6,en;q=0.4",
}


# --------------------------- Utilitaires texte ---------------------------

def strip_accents(text: str) -> str:
    if not isinstance(text, str):
        return ""
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    txt = " ".join(text.replace("\xa0", " ").split())
    return txt.strip()


def contains_any(haystack: str, needles: List[str]) -> bool:
    base = strip_accents(haystack).lower()
    for n in needles:
        if strip_accents(n).lower() in base:
            return True
    return False


# --------------------------- HTTP / Session ---------------------------

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    # Facultatif: proxies depuis variables d'environnement
    # HTTP(S)_PROXY, http_proxy, https_proxy
    env_proxies = {}
    for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        val = os.getenv(key)
        if val:
            if key.lower().startswith("https"):
                env_proxies["https"] = val
            else:
                env_proxies["http"] = val
    if env_proxies:
        session.proxies = env_proxies

    return session


def fetch_soup(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        print(f"[Erreur] Requête échouée: {url} -> {e}", file=sys.stderr)
        return None

    if resp.status_code != 200:
        print(
            f"[Erreur] Statut HTTP {resp.status_code} pour {url}",
            file=sys.stderr,
        )
        return None

    html = resp.text or ""
    # Détection basique page de failover
    if contains_any(
        html,
        [
            "Access to this site was denied",
            "This website is currently not available",
            "site Web n'est actuellement pas disponible",
            "L'accès à ce site a été refusé",
        ],
    ):
        print(
            f"[Avertissement] Page d'accès refusé/failover détectée pour {url}",
            file=sys.stderr,
        )
        # On renvoie tout de même le soup pour permettre un debug
    return BeautifulSoup(html, "html.parser")


# --------------------------- Parsing sections ---------------------------

IDENTIFICATION_KEYWORDS = [
    "Éléments d'identification",
    "Elements d'identification",
    "Elements d identification",
    "Éléments d’identification",  # apostrophe typographique
    "Identification",
]

QUALIFICATION_KEYWORDS = [
    "Qualification de l'infraction",
    "Qualification de l’infraction",
    "Qualification des infractions",
    "Qualification de(s) l'infraction(s)",
    "Infraction",
    "Infractions",
    "Offence",
    "Offences",
    "Charges",
]


def find_section_by_heading(soup: BeautifulSoup, keywords: List[str]) -> Optional[BeautifulSoup]:
    # Chercher h1..h5 contenant les mots-clés
    for tag_name in ["h1", "h2", "h3", "h4", "h5"]:
        for h in soup.find_all(tag_name):
            title = clean_text(h.get_text(" ", strip=True))
            if not title:
                continue
            if contains_any(title, keywords):
                # Idéalement la section est le parent le plus proche significatif
                # On privilégie un parent <section> ou <div> structurant
                for parent in [h.parent, h.find_parent("section"), h.find_parent("div")]:
                    if parent and len(clean_text(parent.get_text(" ", strip=True))) > len(title):
                        return parent
                return h
    return None


def extract_pairs_from_dl(container: BeautifulSoup) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for dl in container.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for i in range(min(len(dts), len(dds))):
            key = clean_text(dts[i].get_text(" ", strip=True))
            val = clean_text(dds[i].get_text(" ", strip=True))
            if key or val:
                pairs.append((key, val))
    return pairs


def extract_pairs_from_tables(container: BeautifulSoup) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for table in container.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [clean_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            if len(cells) >= 2:
                key = cells[0]
                val = " ".join(cells[1:])
                if key or val:
                    pairs.append((key, val))
    return pairs


def extract_pairs_from_list_items(container: BeautifulSoup) -> List[Tuple[str, str]]:
    # Cherche des contenus du type "Label: valeur" dans des <li> / <p>
    pairs: List[Tuple[str, str]] = []
    for el in container.find_all(["li", "p"]):
        txt = clean_text(el.get_text(" ", strip=True))
        if not txt or ":" not in txt:
            continue
        parts = [p.strip() for p in txt.split(":", 1)]
        if len(parts) == 2:
            key, val = parts
            if key or val:
                pairs.append((key, val))
    return pairs


def normalize_identification_key(raw_key: str) -> str:
    key = strip_accents(raw_key).lower().strip()
    # Normalisation de quelques clés courantes (FR et EN)
    mapping = {
        "nom": "Nom",
        "surname": "Nom",
        "nom de famille": "Nom",
        "prenom": "Prénom",
        "prenoms": "Prénom",
        "forename": "Prénom",
        "given names": "Prénom",
        "sexe": "Sexe",
        "sex": "Sexe",
        "date de naissance": "Date de naissance",
        "date of birth": "Date de naissance",
        "lieu de naissance": "Lieu de naissance",
        "place of birth": "Lieu de naissance",
        "pays de naissance": "Pays de naissance",
        "country of birth": "Pays de naissance",
        "nationalite": "Nationalité",
        "nationalites": "Nationalité",
        "nationality": "Nationalité",
        "nationalities": "Nationalité",
        "taille": "Taille (cm)",
        "height": "Taille (cm)",
        "poids": "Poids (kg)",
        "weight": "Poids (kg)",
        "couleur des yeux": "Couleur des yeux",
        "eyes": "Couleur des yeux",
        "couleur des cheveux": "Couleur des cheveux",
        "hair": "Couleur des cheveux",
        "langues": "Langues",
        "languages": "Langues",
        "alias": "Alias",
        "aliases": "Alias",
        "identite": "Identité",
        "identites": "Identité",
    }
    # Mapping exact si possible
    if key in mapping:
        return mapping[key]
    # Essais approximatifs
    if "yeux" in key or "eyes" in key:
        return "Couleur des yeux"
    if "cheveux" in key or "hair" in key:
        return "Couleur des cheveux"
    if "nation" in key:
        return "Nationalité"
    if "birth" in key or "naissance" in key:
        if "lieu" in key or "place" in key:
            return "Lieu de naissance"
        if "pays" in key or "country" in key:
            return "Pays de naissance"
        return "Date de naissance"
    return raw_key.strip()


def extract_identification(container: BeautifulSoup) -> Dict[str, str]:
    # Agrège des paires clé/valeur depuis différents patterns HTML
    pairs: List[Tuple[str, str]] = []
    pairs.extend(extract_pairs_from_dl(container))
    pairs.extend(extract_pairs_from_tables(container))
    pairs.extend(extract_pairs_from_list_items(container))

    data: Dict[str, str] = {}
    for raw_key, raw_val in pairs:
        key = normalize_identification_key(raw_key)
        val = clean_text(raw_val)
        if not val:
            continue
        # Concaténer si clé déjà présente
        if key in data and data[key] and val not in data[key]:
            data[key] = f"{data[key]}; {val}"
        else:
            data[key] = val
    return data


def extract_qualification(container: BeautifulSoup) -> str:
    # Récupère une synthèse textuelle des infractions/charges
    bullets: List[str] = []
    for li in container.find_all("li"):
        txt = clean_text(li.get_text(" ", strip=True))
        if txt:
            bullets.append(txt)
    if not bullets:
        # Pas de listes, on prend paragraphes
        for p in container.find_all("p"):
            txt = clean_text(p.get_text(" ", strip=True))
            if txt:
                bullets.append(txt)
    summary = "; ".join(bullets)
    return summary[:1000]


def compute_age_from_date_text(date_text: str) -> str:
    # Attendu formats variés -> on ne prend que l'année si possible
    if not date_text:
        return ""
    digits = [ch for ch in date_text if ch.isdigit()]
    if len(digits) < 4:
        return ""
    try:
        year = int("".join(digits[:4]))
    except Exception:
        return ""
    from datetime import datetime as _dt

    current_year = _dt.now().year
    if 1900 <= year <= current_year:
        age = current_year - year
        return str(age) if age > 0 else ""
    return ""


# --------------------------- Scraping notice ---------------------------

@dataclass
class Notice:
    url: str
    fields: Dict[str, str]
    infractions: str

    def to_row(self) -> Dict[str, str]:
        # Colonnes normalisées
        base = {
            "URL": self.url,
            "Nom": self.fields.get("Nom", ""),
            "Prénom": self.fields.get("Prénom", ""),
            "Sexe": self.fields.get("Sexe", ""),
            "Date de naissance": self.fields.get("Date de naissance", ""),
            "Âge": "",
            "Nationalité": self.fields.get("Nationalité", ""),
            "Lieu de naissance": self.fields.get("Lieu de naissance", ""),
            "Pays de naissance": self.fields.get("Pays de naissance", ""),
            "Taille (cm)": self.fields.get("Taille (cm)", ""),
            "Poids (kg)": self.fields.get("Poids (kg)", ""),
            "Couleur des yeux": self.fields.get("Couleur des yeux", ""),
            "Couleur des cheveux": self.fields.get("Couleur des cheveux", ""),
            "Langues": self.fields.get("Langues", ""),
            "Alias": self.fields.get("Alias", ""),
            "Infractions": self.infractions,
        }
        # Calcule l'âge si possible
        base["Âge"] = compute_age_from_date_text(base.get("Date de naissance", ""))

        # Ajouter autres champs identification en vrac si non captés
        extras: List[str] = []
        for k, v in self.fields.items():
            if k in base and base[k]:
                continue
            if k in ("Nom", "Prénom", "Sexe", "Date de naissance", "Lieu de naissance", "Pays de naissance",
                     "Nationalité", "Taille (cm)", "Poids (kg)", "Couleur des yeux", "Couleur des cheveux",
                     "Langues", "Alias"):
                continue
            if v:
                extras.append(f"{k}: {v}")
        base["Identification - autres"] = "; ".join(extras)[:1000]
        return base


def scrape_notice(session: requests.Session, url: str) -> Optional[Notice]:
    soup = fetch_soup(session, url)
    if not soup:
        return None

    ident_container = find_section_by_heading(soup, IDENTIFICATION_KEYWORDS)
    qualif_container = find_section_by_heading(soup, QUALIFICATION_KEYWORDS)

    fields: Dict[str, str] = {}
    infractions_text = ""

    if ident_container:
        fields = extract_identification(ident_container)
    else:
        print(
            f"[Info] Section 'Éléments d'identification' introuvable pour {url}",
            file=sys.stderr,
        )

    if qualif_container:
        infractions_text = extract_qualification(qualif_container)
    else:
        print(
            f"[Info] Section 'Qualification de l'infraction' introuvable pour {url}",
            file=sys.stderr,
        )

    return Notice(url=url, fields=fields, infractions=infractions_text)


# --------------------------- I/O helpers ---------------------------

def read_urls_file(path: str) -> List[str]:
    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            urls.append(url)
    return urls


def write_csv(rows: List[Dict[str, str]], output_path: str) -> None:
    if not rows:
        print("[Erreur] Aucune donnée à écrire.", file=sys.stderr)
        return

    # Colonnes stables + autres si existantes
    base_fields = [
        "URL",
        "Nom",
        "Prénom",
        "Sexe",
        "Date de naissance",
        "Âge",
        "Nationalité",
        "Lieu de naissance",
        "Pays de naissance",
        "Taille (cm)",
        "Poids (kg)",
        "Couleur des yeux",
        "Couleur des cheveux",
        "Langues",
        "Alias",
        "Infractions",
        "Identification - autres",
    ]
    # Inclure automatiquement des champs qui auraient été découverts
    extra_keys: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in base_fields and k not in extra_keys:
                extra_keys.append(k)
    fieldnames = base_fields + extra_keys

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"[OK] {len(rows)} notices écrites dans: {output_path}")


# --------------------------- CLI ---------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scraper Interpol (HTML) - extraction des 'Éléments d'identification' "
            "et 'Qualification de l'infraction' depuis des URLs de notice."
        )
    )
    parser.add_argument(
        "--urls-file",
        type=str,
        required=True,
        help=(
            "Chemin d'un fichier texte contenant une URL de notice par ligne. "
            "Ces pages doivent être les pages DÉTAIL (pas la page de liste)."
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Fichier CSV de sortie (défaut: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help=f"Délai (secondes) entre requêtes (défaut: {DEFAULT_DELAY_SECONDS})",
    )

    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    if not os.path.isfile(args.urls_file):
        print(f"[Erreur] Fichier introuvable: {args.urls_file}", file=sys.stderr)
        return 1

    urls = read_urls_file(args.urls_file)
    if not urls:
        print("[Erreur] Aucune URL à traiter.", file=sys.stderr)
        return 1

    session = create_session()

    rows: List[Dict[str, str]] = []
    processed = 0
    for url in urls:
        print(f"[Info] Traitement: {url}", file=sys.stderr)
        notice = scrape_notice(session, url)
        if notice:
            rows.append(notice.to_row())
            processed += 1
        else:
            print(f"[Avertissement] Échec de l'extraction pour: {url}", file=sys.stderr)
        time.sleep(max(0.0, float(args.delay)))

    if not rows:
        print("[Erreur] Aucune notice extraite.", file=sys.stderr)
        return 1

    write_csv(rows, args.output)
    print(f"[OK] Terminé. Notices extraites: {processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

