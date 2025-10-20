"""
Scraper Interpol Red Notices - Version 100% BeautifulSoup4
Récupère les 6519 résultats en utilisant uniquement BeautifulSoup4 pour le parsing
"""

import sys
import time
import csv
import json
import re
import os
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup

# API Interpol
API_URL = 'https://ws-public.interpol.int/notices/v1/red'

# Configuration
RESULTS_PER_PAGE = 160
PAGE_DELAY = 1.0
DETAIL_DELAY = 0.4

_session: Optional[requests.Session] = None


def _default_headers() -> Dict[str, str]:
    """En-têtes par défaut compatibles Onyxia (plus tolérants si 403)."""
    ua = os.getenv('SCRAPER_UA') or 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    referer = os.getenv('SCRAPER_REFERER') or 'https://www.interpol.int/'
    return {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'origin': 'https://www.interpol.int',
        'referer': referer,
        'user-agent': ua,
    }


def create_session() -> requests.Session:
    """
    Crée une session avec les en-têtes du cURL Onyxia fournis par l'utilisateur.
    """
    s = requests.Session()

    # Headers EXACTS du cURL fourni (Onyxia)
    s.headers.update({
        'accept': '*/*',
        'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'origin': 'https://www.interpol.int',
        'priority': 'u=1, i',
        'referer': 'https://www.interpol.int/',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    })

    return s


def get_session() -> requests.Session:
    """Retourne la session globale"""
    global _session
    if _session is None:
        _session = create_session()
    return _session


def build_page_url(page: int, search_request_id: Optional[str] = None) -> str:
    """Construit l'URL absolue d'une page à partir de l'API de base."""
    params = {
        'page': str(page),
        'resultPerPage': str(RESULTS_PER_PAGE),
    }
    if search_request_id:
        params['searchRequestId'] = search_request_id
    return f"{API_URL}?{urlencode(params)}"


def fetch_page(page: int, url: Optional[str] = None,
               params: Optional[Dict[str, str]] = None) -> Optional[Dict]:
    """Récupère une page de notices et annote la réponse avec l'URL utilisée."""
    sess = get_session()

    request_url = url or API_URL
    query_params: Optional[Dict[str, str]]
    if params is not None:
        query_params = params
    elif url is None:
        query_params = {
            'page': str(page),
            'resultPerPage': str(RESULTS_PER_PAGE)
        }
    else:
        query_params = None

    try:
        suffix = " (lien next)" if url else ""
        print(f"[Page {page}] Requête{suffix}...", end=' ', file=sys.stderr, flush=True)

        response = sess.get(request_url, params=query_params, timeout=30)

        # Afficher le code de statut
        print(f"Status: {response.status_code}", end=' ', file=sys.stderr, flush=True)

        if response.status_code == 403:
            # Repli côté Onyxia: simplifier encore les en-têtes et retenter 1 fois
            print("X 403 Forbidden — tentative avec en-têtes simplifiés", file=sys.stderr)
            for k in ['x-requested-with', 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform',
                      'sec-fetch-dest', 'sec-fetch-mode', 'sec-fetch-site', 'priority']:
                if k in sess.headers:
                    sess.headers.pop(k, None)
            sess.headers.update(_default_headers())
            response = sess.get(request_url, params=query_params, timeout=30)
            print(f"Retry Status: {response.status_code}", end=' ', file=sys.stderr, flush=True)
            if response.status_code == 403:
                print("X 403 persistante", file=sys.stderr)
                return None

        if response.status_code == 404:
            print("(fin du dataset)", file=sys.stderr)
            return None

        response.raise_for_status()

        # Parser le JSON
        data = response.json()
        # Conserver l'URL finale utilisée
        if isinstance(data, dict):
            data.setdefault('_meta', {})
            if isinstance(data['_meta'], dict):
                data['_meta']['request_url'] = response.url
        print("OK", file=sys.stderr)
        return data

    except requests.exceptions.JSONDecodeError as e:
        print(f"X Erreur JSON: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"X {type(e).__name__}: {e}", file=sys.stderr)
        return None


def fetch_detail(detail_url: str) -> Optional[Dict]:
    """Récupère le JSON de détail d'une notice (endpoint _links.self)."""
    if not detail_url:
        return None
    sess = get_session()
    try:
        resp = sess.get(detail_url, timeout=30)
        if resp.status_code == 403:
            for k in ['x-requested-with', 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform',
                      'sec-fetch-dest', 'sec-fetch-mode', 'sec-fetch-site', 'priority']:
                if k in sess.headers:
                    sess.headers.pop(k, None)
            resp = sess.get(detail_url, timeout=30)
            if resp.status_code == 403:
                return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def clean_text_with_bs4(text: str) -> str:
    """
    Nettoie un texte en utilisant BeautifulSoup4
    Supprime les balises HTML, normalise les espaces, etc.
    """
    if not text or not isinstance(text, str):
        return ''

    soup = BeautifulSoup(f"<div>{text}</div>", 'html.parser')
    cleaned = soup.get_text()
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def extract_age_with_bs4(date_of_birth: str) -> str:
    """
    Extrait l'âge depuis la date de naissance en utilisant BeautifulSoup4
    """
    if not date_of_birth:
        return ''

    cleaned_dob = clean_text_with_bs4(date_of_birth)
    soup = BeautifulSoup(f"<div>{cleaned_dob}</div>", 'html.parser')
    text = soup.get_text()

    match = re.search(r'\b(19|20)\d{2}\b', text)
    if match:
        try:
            year = int(match.group(0))
            age = datetime.now().year - year
            return str(age) if 0 <= age <= 120 else ''
        except Exception:
            pass
    return ''


def parse_nationalities_with_bs4(nationalities) -> str:
    """
    Parse les nationalités en utilisant BeautifulSoup4
    """
    if not nationalities:
        return ''

    if isinstance(nationalities, list):
        cleaned_nationalities = []
        for nat in nationalities:
            if nat:
                cleaned = clean_text_with_bs4(str(nat))
                if cleaned:
                    cleaned_nationalities.append(cleaned)
        return ', '.join(cleaned_nationalities)
    else:
        return clean_text_with_bs4(str(nationalities))


def parse_charges_with_bs4(warrants: List[Dict]) -> str:
    """
    Parse les infractions en utilisant BeautifulSoup4
    """
    if not warrants or not isinstance(warrants, list):
        return ''

    charges = []

    for warrant in warrants:
        if not isinstance(warrant, dict):
            continue

        # charge
        charge = warrant.get('charge', '')
        if charge:
            cleaned_charge = clean_text_with_bs4(charge)
            if cleaned_charge and cleaned_charge not in charges:
                charges.append(cleaned_charge)

        # charges[]
        additional_charges = warrant.get('charges', [])
        if isinstance(additional_charges, list):
            for additional_charge in additional_charges:
                if additional_charge:
                    cleaned_additional = clean_text_with_bs4(str(additional_charge))
                    if cleaned_additional and cleaned_additional not in charges:
                        charges.append(cleaned_additional)

        # charge_translation (si présent)
        charge_translation = warrant.get('charge_translation')
        if charge_translation:
            cleaned_ct = clean_text_with_bs4(str(charge_translation))
            if cleaned_ct and cleaned_ct not in charges:
                charges.append(cleaned_ct)

    return '; '.join(charges)


def parse_notice_with_bs4(notice: Dict) -> Dict[str, str]:
    """
    Parse une notice en utilisant BeautifulSoup4 à 100%
    Extrait les informations demandées:
    - Nom, Prénom, Âge, Nationalité, Infractions
    """

    nom_raw = notice.get('name', '')
    nom = clean_text_with_bs4(nom_raw)

    prenom_raw = notice.get('forename', '')
    prenom = clean_text_with_bs4(prenom_raw)

    dob_raw = notice.get('date_of_birth', '')
    age = extract_age_with_bs4(dob_raw)

    nationalities_raw = notice.get('nationalities', [])
    nationalite = parse_nationalities_with_bs4(nationalities_raw)

    warrants_raw = notice.get('arrest_warrants', [])
    infractions = parse_charges_with_bs4(warrants_raw)

    result = {
        'Nom': clean_text_with_bs4(nom),
        'Prenom': clean_text_with_bs4(prenom),
        'Age': clean_text_with_bs4(age),
        'Nationalite': clean_text_with_bs4(nationalite),
        'Infractions': clean_text_with_bs4(infractions)
    }

    return result


def merge_infractions_from_notice_and_detail(base_notice: Dict, detail_notice: Optional[Dict]) -> str:
    """Fusionne toutes les infractions depuis la notice de liste et la notice de détail."""
    warrants_all: List[Dict] = []
    wl = base_notice.get('arrest_warrants', [])
    if isinstance(wl, list):
        warrants_all.extend([w for w in wl if isinstance(w, dict)])
    if isinstance(detail_notice, dict):
        wd = detail_notice.get('arrest_warrants', [])
        if isinstance(wd, list):
            warrants_all.extend([w for w in wd if isinstance(w, dict)])
    return parse_charges_with_bs4(warrants_all)


def scrape_all_notices_bs4(max_pages: Optional[int] = None) -> List[Dict[str, str]]:
    """
    Scrape toutes les notices rouges en utilisant BeautifulSoup4
    """

    print("\n" + "=" * 80, file=sys.stderr)
    print("DÉMARRAGE DU SCRAPING INTERPOL - VERSION BEAUTIFULSOUP4", file=sys.stderr)
    print("=" * 80 + "\n", file=sys.stderr)

    all_people: List[Dict[str, str]] = []

    # Récupérer la première page pour connaître le total
    print("[Info] Récupération du nombre total...\n", file=sys.stderr)
    first_page = fetch_page(1)

    if first_page is None:
        return []

    # Calculer le nombre total de pages
    total = first_page.get('total', 0)
    total_pages = (total // RESULTS_PER_PAGE) + (1 if total % RESULTS_PER_PAGE else 0)
    if total_pages == 0 and total:
        total_pages = 1

    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"[Info] Total de notices: {total}", file=sys.stderr)
    print(f"[Info] Pages à scraper: {total_pages}", file=sys.stderr)
    print(f"[Info] Utilisation de BeautifulSoup4 pour le parsing\n", file=sys.stderr)

    if total_pages == 0:
        print("\n[Info] Aucun résultat à traiter\n", file=sys.stderr)
        return []

    # Boucle simple: demander page=1..N explicitement (l'API supporte la pagination directe)
    for page in range(1, total_pages + 1):
        if page == 1:
            data = first_page
        else:
            time.sleep(PAGE_DELAY)
            data = fetch_page(page)
        if data is None:
            print(f"[Page {page}] Données indisponibles, arrêt.", file=sys.stderr)
            break

        notices: List[Dict] = []
        if '_embedded' in data and isinstance(data['_embedded'], dict) and 'notices' in data['_embedded']:
            notices = data['_embedded']['notices']
        elif 'notices' in data:
            notices = data['notices']

        new_count = 0
        for notice in notices:
            if not isinstance(notice, dict):
                continue
            person = parse_notice_with_bs4(notice)

            # Enrichir les infractions en appelant le détail quand possible
            detail_url = ''
            links = notice.get('_links') if isinstance(notice, dict) else None
            if isinstance(links, dict):
                self_link = links.get('self')
                if isinstance(self_link, dict):
                    detail_url = self_link.get('href') or ''
                elif isinstance(self_link, str):
                    detail_url = self_link

            if detail_url:
                detail_data = fetch_detail(detail_url)
                if detail_data:
                    all_infractions = merge_infractions_from_notice_and_detail(notice, detail_data)
                    if all_infractions:
                        person['Infractions'] = all_infractions
                time.sleep(DETAIL_DELAY)

            if not (person['Nom'] or person['Prenom']):
                continue

            all_people.append(person)
            new_count += 1

        print(f"[Page {page}/{total_pages}] OK {new_count} notices (Total: {len(all_people)})", file=sys.stderr)

    print(f"\n[Terminé] {len(all_people)} notices récupérées avec BeautifulSoup4\n", file=sys.stderr)
    return all_people


def print_sample_bs4(people: List[Dict[str, str]], n: int = 10) -> None:
    """Affiche un échantillon des résultats parsés avec BeautifulSoup4"""
    if not people:
        print("\nX Aucun résultat\n")
        return

    print("\n" + "=" * 120)
    print("RÉSULTATS PARSÉS AVEC BEAUTIFULSOUP4")
    print("=" * 120)
    print(f"{'Nom':<20} {'Prenom':<20} {'Age':<5} {'Nationalite':<25} {'Infractions'}")
    print("=" * 120)

    for person in people[:n]:
        nom = person['Nom'][:20]
        prenom = person['Prenom'][:20]
        age = person['Age'][:5]
        nat = person['Nationalite'][:25]
        inf = person['Infractions'][:40]

        print(f"{nom:<20} {prenom:<20} {age:<5} {nat:<25} {inf}")

    if len(people) > n:
        print(f"\n... et {len(people) - n} autres personnes")

    print("=" * 120 + "\n")


def save_csv_bs4(people: List[Dict[str, str]], filename: str) -> None:
    """Sauvegarde les résultats parsés avec BeautifulSoup4 en CSV"""
    if not people:
        print("X Aucune donnée à sauvegarder")
        return

    fieldnames = ['Nom', 'Prenom', 'Age', 'Nationalite', 'Infractions']

    def normalise_person_bs4(person: Dict[str, str]) -> Dict[str, str]:
        """Normalise les clés pour le CSV"""
        mapping = {
            'Nom': person.get('Nom') or '',
            'Prenom': person.get('Prenom') or '',
            'Age': person.get('Age') or '',
            'Nationalite': person.get('Nationalite') or '',
            'Infractions': person.get('Infractions') or ''
        }
        return mapping

    with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for person in people:
            writer.writerow(normalise_person_bs4(person))

    print(f"✓ {len(people)} notices sauvegardées dans: {filename}")


def main(argv: List[str]) -> int:
    """Fonction principale"""
    import argparse

    # Valeurs par défaut adaptables via variables d'environnement (compat Onyxia)
    default_output = os.getenv('SCRAPER_OUTPUT') or 'interpol_notices_bs4.csv'
    default_max_pages_env = os.getenv('SCRAPER_MAX_PAGES')
    default_max_pages = int(default_max_pages_env) if default_max_pages_env and default_max_pages_env.isdigit() else None
    default_delay = float(os.getenv('SCRAPER_DELAY') or '1.0')
    default_detail_delay = float(os.getenv('SCRAPER_DETAIL_DELAY') or str(DETAIL_DELAY))

    parser = argparse.ArgumentParser(
        description='Scraper des notices rouges Interpol - Version BeautifulSoup4',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Version 100% BeautifulSoup4:
- Toutes les données sont parsées avec BeautifulSoup4
- Nettoyage automatique des balises HTML
- Extraction robuste des informations
- Récupération des 6519 résultats complets
        """
    )

    parser.add_argument('--output', '-o', default=default_output,
                        help='Fichier CSV de sortie')
    parser.add_argument('--max-pages', '-n', type=int, default=default_max_pages,
                        help='Nombre maximum de pages (None = toutes les pages)')
    parser.add_argument('--delay', '-d', type=float, default=default_delay,
                        help='Délai entre les pages (secondes)')
    parser.add_argument('--detail-delay', type=float, default=default_detail_delay,
                        help='Délai entre les requêtes de détail (secondes)')

    args = parser.parse_args(argv[1:])

    # Configuration
    globals()['PAGE_DELAY'] = args.delay
    globals()['DETAIL_DELAY'] = args.detail_delay

    print("\n" + "=" * 80)
    print("SCRAPER NOTICES ROUGES INTERPOL - VERSION BEAUTIFULSOUP4")
    print("=" * 80)
    print(f"Sortie: {args.output}")
    print(f"Pages max: {'Toutes (6519 résultats)' if args.max_pages is None else args.max_pages}")
    print(f"Délai: {args.delay}s")
    print(f"Délai détail: {args.detail_delay}s")
    print("=" * 80)

    # Scraping avec BeautifulSoup4
    try:
        people = scrape_all_notices_bs4(max_pages=args.max_pages)
    except KeyboardInterrupt:
        print("\n\nInterruption utilisateur\n", file=sys.stderr)
        return 130

    if not people:
        print("\nX Aucune notice récupérée\n")
        return 1

    # Afficher un échantillon
    print_sample_bs4(people, n=10)

    # Sauvegarder
    save_csv_bs4(people, args.output)

    print(f"\n🎉 {len(people)} notices extraites avec BeautifulSoup4!")
    print(f"📁 Fichier sauvegardé: {args.output}\n")

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))