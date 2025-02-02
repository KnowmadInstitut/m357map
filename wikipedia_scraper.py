# -*- coding: utf-8 -*-
"""
wikipedia_scraper.py

Script avanzado para:
1) Buscar artículos relacionados con masonería en múltiples Wikipedias.
2) Obtener detalles del artículo (resumen, categorías) + QID de Wikidata.
3) Extraer coordenadas de P625 (Wikidata) o bien con infobox -> geocodificación.
4) Analizar fechas históricas (dateparser) y generar referencias APA.
5) Mantener un sistema de caché (en JSON o SQLite) para no reprocesar datos.
6) Concurrencia usando ThreadPoolExecutor.

TODO:
- Ajustar KEYWORDS y WIKI_LANGUAGES según tus necesidades.
- Revisar la sección "Análisis Post-proceso" para personalizar tu analítica.
"""

import requests
import json
import logging
import time
import re
import os
from typing import List, Dict, Optional, Tuple
import mwparserfromhell
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from unidecode import unidecode

# Librerías gratuitas y abiertas
import dateparser  # Para parsear fechas en múltiples idiomas
from tenacity import retry, stop_after_attempt, wait_exponential  # Para reintentos automáticos en requests

# geopy (open source)
from geopy.geocoders import Nominatim

############################################################################
# =========================== CONFIGURACIÓN ================================
############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(module)s] - %(message)s",
    handlers=[
        logging.FileHandler("masonic_research_advanced.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MasonicWikiScraper")

class Config:
    """
    Parametrizaciones básicas:
    - WIKI_LANGUAGES: idiomas de Wikipedia a recorrer.
    - MAX_WORKERS: número de hilos concurrentes en ThreadPoolExecutor.
    - REQUEST_TIMEOUT: tiempo máximo de espera por request.
    - USE_SQLITE_CACHE: si es True, se usará SQLite en vez de JSON para caché.
    - HISTORIC_DATES_REGEX: regex básico para encontrar posibles fechas en el texto.
    - REQUEST_DELAY: retardo entre requests para no saturar los servicios abiertos.
    """

    WIKI_LANGUAGES = ["en", "es"]  # Amplía con 'fr', 'de', 'pt', etc. si deseas
    MAX_WORKERS = 5
    REQUEST_TIMEOUT = 15
    USE_SQLITE_CACHE = False  # Si True, usa SQLite. Si False, usa JSON
    JSON_CACHE_FILE = "scraper_cache.json"
    SQLITE_DB = "scraper_cache.db"
    HISTORIC_DATES_REGEX = r"\b(\d{1,2}\s+\w+\s+\d{4}|\d{4})\b"
    REQUEST_DELAY = 1.0

config = Config()
geolocator = Nominatim(user_agent="masonic_research_v3")

############################################################################
# ============== LISTA DE PALABRAS CLAVE (AMPLÍA SEGÚN NECESIDAD) =========
############################################################################

KEYWORDS = [
    "Freemason", "Mason", "Francmason", "Freemasonry", "Francmasonería", "Gran Logia", "Masonic Lodge",
    "Masonic Temple", "Loge maçonnique", "Freimaurer", "Freimaurerei", "Franc-maçon", "Masonic Order",
    "Grand Orient", "Ancient and Accepted Scottish Rite", "Rito Escocés Antiguo y Aceptado", "York Rite",
    "Rito de York", "Knights Templar", "Caballeros Templarios", "Chevaliers du Temple", "Cavaleiros Templários",
    "Quatuor Coronati", "Hiram Abiff", "Anderson's Constitutions", "Constituciones de Anderson",
    "Constitutions d’Anderson", "Operative Masonry", "Maçonnerie Opérative", "Operative Maurerei",
    "Maçonnerie Opérative", "Free-Masons", "Franc-Maçons", "Franc-Masones", "Estatutos de Schaw",
    "Schaw Statutes", "Schaw-Statuten", "Lodge of Antiquity", "Logia de la Antigüedad", "Loge de l’Antiquité",
    "Mother Kilwinning", "Mãe Kilwinning", "Mère Kilwinning", "Entered Apprentice", "Aprendiz Masón",
    "Apprenti Maçon", "Lehrling", "Templar Freemasonry", "Masonería Templaria", "Maçonnerie Templière",
    "Templer-Freimaurerei", "Regius Manuscript", "Manuscrito Regius", "Manuscrit Regius", "Egregor",
    "Égrégore", "Royal Arch Masonry", "Real Arco Masónico", "Arco Real Maçônico", "Arc Royal Maçonnique",
    "Schottische Grade", "Grados del Rito Escocés", "Graus do Rito Escocês", "Degrés du Rite Écossais",
    "Brotherhood of Light", "Hermandad de la Luz", "Irmandade da Luz", "Fraternité de la Lumière",
    "Symbolic Masonry", "Masonería Simbólica", "Maçonnerie Symbolique", "Symbolische Maurerei",
    "Gothic Cathedral and Masonry", "Catedral Gótica y Masonería", "Cathédrale Gothique et Maçonnerie",
    "Gotische Kathedralen und Freimaurerei", "Speculative Masonry", "Masonería Especulativa",
    "Maçonnerie Spéculative", "Spekulative Maurerei", "Latin American Freemasonry", "Masonería en América Latina",
    "Maçonnerie en Amérique Latine", "Maçonaria na América Latina", "Landmarks of Freemasonry",
    "Landmarks Masónicos", "Landmarks der Freimaurerei", "Landmarks Maçônicos", "Landmarks Maçonniques",
    "Grand Orient of France", "Gran Oriente de Francia", "Grand Orient de France", "Grande Oriente da França",
    "Rectified Scottish Rite", "Rito Escocés Rectificado", "Rite Écossais Rectifié", "Rito Escocês Retificado",
    "Wilhelmsbad Convention", "Convención de Wilhelmsbad", "Convenção de Wilhelmsbad", "Convention de Wilhelmsbad",
    "Ramsay's Oration", "Oración de Ramsay", "Oração de Ramsay", "Discours de Ramsay", "Ramsays Rede",
    "Lessing and German Freemasonry", "Lessing y la Masonería Alemana", "Lessing e a Maçonaria Alemã",
    "Lessing et la Franc-maçonnerie allemande", "Royal Art", "Arte Real", "Art Royal", "Königliche Kunst", 
    # ... añade las palabras que desees
]

############################################################################
# =================== SISTEMA DE CACHÉ: JSON O SQLITE ======================
############################################################################

def ensure_sqlite_tables(conn):
    """Crea tablas (articles, locations) si no existen."""
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            cache_key TEXT PRIMARY KEY,
            data TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            place_name TEXT PRIMARY KEY,
            lat REAL,
            lon REAL
        )
    """)
    conn.commit()

class CacheManager:
    """
    Manejador de caché.
    - Por defecto usa un archivo JSON (scraper_cache.json).
    - Si se activa USE_SQLITE_CACHE, utiliza una base de datos local 'scraper_cache.db'.
    """

    @staticmethod
    def load_json() -> Dict:
        if not os.path.exists(config.JSON_CACHE_FILE):
            return {"articles": {}, "locations": {}}
        try:
            with open(config.JSON_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"articles": {}, "locations": {}}

    @staticmethod
    def save_json(cache: Dict):
        with open(config.JSON_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)

    @staticmethod
    def load_sqlite():
        import sqlite3
        conn = sqlite3.connect(config.SQLITE_DB)
        ensure_sqlite_tables(conn)
        return conn

    @staticmethod
    def get_article(cache_key: str) -> Optional[Dict]:
        if config.USE_SQLITE_CACHE:
            import sqlite3
            conn = CacheManager.load_sqlite()
            c = conn.cursor()
            c.execute("SELECT data FROM articles WHERE cache_key = ?", (cache_key,))
            row = c.fetchone()
            conn.close()
            if row:
                return json.loads(row[0])
            return None
        else:
            # JSON
            c = CacheManager.load_json()
            return c["articles"].get(cache_key)

    @staticmethod
    def set_article(cache_key: str, data: Dict):
        if config.USE_SQLITE_CACHE:
            import sqlite3
            conn = CacheManager.load_sqlite()
            c = conn.cursor()
            c.execute("REPLACE INTO articles (cache_key, data) VALUES (?, ?)",
                      (cache_key, json.dumps(data, ensure_ascii=False)))
            conn.commit()
            conn.close()
        else:
            c = CacheManager.load_json()
            c["articles"][cache_key] = data
            CacheManager.save_json(c)

    @staticmethod
    def get_location(place_name: str) -> Optional[Tuple[float, float]]:
        if config.USE_SQLITE_CACHE:
            import sqlite3
            conn = CacheManager.load_sqlite()
            c = conn.cursor()
            c.execute("SELECT lat, lon FROM locations WHERE place_name = ?", (place_name,))
            row = c.fetchone()
            conn.close()
            if row:
                return (row[0], row[1])
            return None
        else:
            c = CacheManager.load_json()
            loc = c["locations"].get(place_name)
            if loc:
                return (loc["lat"], loc["lon"])
            return None

    @staticmethod
    def set_location(place_name: str, lat: float, lon: float):
        if config.USE_SQLITE_CACHE:
            import sqlite3
            conn = CacheManager.load_sqlite()
            c = conn.cursor()
            c.execute("REPLACE INTO locations (place_name, lat, lon) VALUES (?, ?, ?)",
                      (place_name, lat, lon))
            conn.commit()
            conn.close()
        else:
            c = CacheManager.load_json()
            c["locations"][place_name] = {"lat": lat, "lon": lon}
            CacheManager.save_json(c)

############################################################################
# =============== BÚSQUEDA AVANZADA (MediaWiki / CirrusSearch) =============
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def advanced_search(keyword: str, lang: str) -> List[Dict]:
    """
    Realiza una búsqueda avanzada en Wikipedia usando CirrusSearch.
    Para no saturar con muchas OR, aquí se demuestra un ejemplo con incategory/hastemplate.
    """
    # Por ej.: "(incategory:'Masonic_buildings' OR hastemplate:'Infobox_freemasonry')"
    srsearch = f'"{keyword}" AND (incategory:"Masonic_buildings" OR hastemplate:"Infobox_freemasonry")'
    params = {
        "action": "query",
        "list": "search",
        "srsearch": srsearch,
        "format": "json",
        "srlimit": 10,
        "srprop": "size|wordcount|timestamp"
    }
    url = f"https://{lang}.wikipedia.org/w/api.php"
    resp = requests.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
    resp.raise_for_status()

    results = resp.json().get("query", {}).get("search", [])
    # Ordenar por wordcount descendente, tomar top 5
    sorted_res = sorted(results, key=lambda x: x.get("wordcount", 0), reverse=True)[:5]
    return sorted_res

############################################################################
# ============= WIKIDATA: QID y Coordenadas P625 ===========================
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def get_qid_from_wikipedia_page(page_title: str, lang: str) -> Optional[str]:
    """Obtiene el QID (p.ej. 'Q12345') de un artículo de Wikipedia."""
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "pageprops",
        "format": "json",
        "titles": page_title
    }
    r = requests.get(api_url, params=params, timeout=config.REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    pages = data["query"]["pages"]
    for _, page_info in pages.items():
        if "pageprops" in page_info and "wikibase_item" in page_info["pageprops"]:
            return page_info["pageprops"]["wikibase_item"]
    return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def get_coords_from_wikidata(qid: str) -> Tuple[Optional[float], Optional[float]]:
    """Retorna (lat, lon) de la propiedad P625 en Wikidata o (None, None) si no existe."""
    if not qid:
        return None, None
    wd_url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    resp = requests.get(wd_url, timeout=config.REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    entity = data["entities"].get(qid, {})
    claims = entity.get("claims", {})
    if "P625" in claims:
        coord_claim = claims["P625"][0]["mainsnak"]["datavalue"]["value"]
        lat = coord_claim["latitude"]
        lon = coord_claim["longitude"]
        return (lat, lon)
    return None, None

############################################################################
# ============== PARSE INFOBOX PARA UBICACIÓN =============================
############################################################################

def parse_infobox_location(page_title: str, lang: str) -> Optional[str]:
    """
    Descarga el wikitext raw y busca campos como birth_place, location, etc.
    """
    raw_url = f"https://{lang}.wikipedia.org/w/index.php?title={page_title}&action=raw"
    try:
        resp = requests.get(raw_url, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None
        wikitext = resp.text
    except requests.RequestException as e:
        logger.warning(f"Error fetch wikitext {page_title} {lang}: {e}")
        return None

    parsed = mwparserfromhell.parse(wikitext)
    templates = parsed.filter_templates()
    campos_ubicacion = [
        "birth_place", "death_place", "headquarters", "location",
        "place", "foundation_place", "venue", "native_place"
    ]
    for tmpl in templates:
        name = tmpl.name.strip().lower()
        if "infobox" in name:
            for campo in campos_ubicacion:
                if tmpl.has(campo):
                    val = tmpl.get(campo).value.strip()
                    if val:
                        return str(val)
    return None

############################################################################
# ============= GEOCODIFICACIÓN: PHOTON + NOMINATIM (GRATIS) ===============
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def enhanced_geocoding(place_name: str) -> Optional[Tuple[float, float]]:
    """
    - Limpia 'place_name' y recurre a Photon y luego Nominatim.
    - Almacena el resultado en caché para no repetir.
    - No se usa MapQuest ni otras APIs privadas.
    """
    if not place_name:
        return None

    cached = CacheManager.get_location(place_name)
    if cached:
        return cached

    # Limpieza
    clean_place = re.sub(r"\(.*?\)", "", place_name).strip()
    clean_place = unidecode(clean_place)

    services = [
        ("photon", "https://photon.komoot.io/api/"),
        ("nominatim", "https://nominatim.openstreetmap.org/search/")
    ]

    for svc, url in services:
        try:
            time.sleep(config.REQUEST_DELAY)
            if svc == "photon":
                r = requests.get(url, params={"q": clean_place}, timeout=10)
                j = r.json()
                if j.get("features"):
                    coords = j["features"][0]["geometry"]["coordinates"]
                    lat, lon = coords[1], coords[0]
                    CacheManager.set_location(place_name, lat, lon)
                    return (lat, lon)
            elif svc == "nominatim":
                r = requests.get(url, params={"q": clean_place, "format": "json"}, timeout=10)
                j = r.json()
                if j:
                    lat = float(j[0]["lat"])
                    lon = float(j[0]["lon"])
                    CacheManager.set_location(place_name, lat, lon)
                    return (lat, lon)
        except Exception as e:
            logger.warning(f"[{svc}] Error geocoding '{clean_place}': {e}")

    logger.error(f"No se pudo geocodificar: {clean_place}")
    return None

############################################################################
# ============ FETCH DETALLES DEL ARTÍCULO: Extract + Categories ===========
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def fetch_article_details(title: str, lang: str = "en") -> Dict:
    """
    Retorna {title, summary, categories}.
    """
    endpoint = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "extracts|categories",
        "exintro": True,
        "explaintext": True,
        "titles": title,
        "format": "json",
        "cllimit": 20
    }
    resp = requests.get(endpoint, params=params, timeout=config.REQUEST_TIMEOUT)
    resp.raise_for_status()

    details = {"title": title, "summary": "", "categories": []}
    pages = resp.json().get("query", {}).get("pages", {})
    for _, pinfo in pages.items():
        details["title"] = pinfo.get("title", title)
        details["summary"] = pinfo.get("extract", "")
        cats = pinfo.get("categories", [])
        details["categories"] = [c.get("title", "") for c in cats]
    return details

############################################################################
# ================ EXTRAER FECHAS HISTÓRICAS (DATEPARSER) =================
############################################################################

def extract_historic_dates(text: str) -> List[Dict]:
    """
    Usa un regex básico para encontrar posibles fechas
    y dateparser para parsear de modo multilingüe.
    """
    pattern = re.compile(config.HISTORIC_DATES_REGEX, re.IGNORECASE)
    matches = pattern.finditer(text)
    results = []

    for m in matches:
        raw_date = m.group(1)
        snippet = text[max(0, m.start()-30): m.end()+30]

        parsed = dateparser.parse(raw_date, settings={'PREFER_DAY_OF_MONTH': 'first'})
        results.append({
            "raw": raw_date,
            "parsed": parsed.isoformat() if parsed else "",
            "context": snippet
        })
    return results

############################################################################
# ========== ENRIQUECER: ENLACES, IMÁGENES Y REFERENCIA APA ================
############################################################################

def add_semantic_links(article_data: Dict):
    """
    Ejemplo: obtiene enlaces que incluyan "masonic" o "freemason" en su título.
    (Puede que en algunos idiomas la API de Wikimedia no lo soporte.)
    """
    lang = article_data.get("lang", "en")
    title_underscore = article_data["title"].replace(" ", "_")
    url = f"https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{title_underscore}/links"
    try:
        r = requests.get(url, timeout=config.REQUEST_TIMEOUT)
        if r.status_code == 200:
            pages = r.json().get("pages", [])
            related = []
            for p in pages:
                tlow = p["title"].lower()
                if "masonic" in tlow or "freemason" in tlow:
                    related.append(p["title"])
            article_data["related_links"] = related
    except Exception as e:
        logger.warning(f"add_semantic_links error: {e}")

def add_image_data(article_data: Dict):
    """
    Obtiene imágenes con 'masonic' o 'lodge' en el título.
    """
    lang = article_data.get("lang", "en")
    t = article_data["title"].replace(" ", "_")
    params = {
        "action": "query",
        "prop": "images",
        "titles": t,
        "format": "json"
    }
    try:
        r = requests.get(f"https://{lang}.wikipedia.org/w/api.php", params=params, timeout=config.REQUEST_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            pages = data["query"]["pages"]
            imgs_ret = []
            for v in pages.values():
                if "images" in v:
                    for img in v["images"]:
                        ititle = img["title"].lower()
                        if "masonic" in ititle or "lodge" in ititle:
                            imgs_ret.append(img["title"])
            article_data["images"] = imgs_ret
    except Exception as e:
        logger.warning(f"add_image_data error: {e}")

def generate_apa_reference(title: str, url: str) -> str:
    """
    Crea una referencia APA 7 simplificada para un artículo de Wikipedia.
    """
    date_str = datetime.now().strftime("%d %B %Y")
    clean_title = title.replace("_", " ")
    return f"{clean_title}. (n.d.). En Wikipedia. Retrieved {date_str}, from {url}"

############################################################################
# ================ PRIORIDAD POR PALABRAS CLAVE EN TÍTULO ==================
############################################################################

def assign_priority(article_data: Dict):
    """
    Ajusta un campo "priority" según ciertas condiciones
    (p. ej., si 'Grand Lodge' aparece en el título).
    """
    title_lower = article_data["title"].lower()
    p = 0
    if "grand lodge" in title_lower:
        p += 50
    if "masonic temple" in title_lower:
        p += 30
    # Otras reglas personalizadas
    article_data["priority"] = p

############################################################################
# ====== FUNCIÓN PIPELINE: PROCESAR 1 ARTÍCULO DE WIKIPEDIA ================
############################################################################

def process_article(article: Dict, keyword: str, lang: str) -> Dict:
    """
    1. Revisa caché
    2. Extrae detalles (resumen, categorías)
    3. QID -> P625 o infobox->geocod
    4. Enlaces e imágenes
    5. Fechas históricas
    6. Referencia APA
    7. Asigna prioridad
    8. Actualiza caché
    """
    cache_key = f"{lang}_{article['title']}"
    cached = CacheManager.get_article(cache_key)
    if cached:
        return cached

    data = {
        "keyword": keyword,
        "lang": lang,
        "title": article["title"],
        "pageid": article.get("pageid"),
        "source_url": f"https://{lang}.wikipedia.org/wiki/{article['title'].replace(' ', '_')}",
        "metrics": {
            "wordcount": article.get("wordcount", 0),
            "last_updated": article.get("timestamp", "")
        }
    }

    # (1) Detalles
    det = fetch_article_details(article["title"], lang=lang)
    data["title"] = det.get("title", data["title"])
    data["summary"] = det.get("summary", "")
    data["categories"] = det.get("categories", [])

    # (2) QID + coords
    qid = get_qid_from_wikipedia_page(data["title"], lang)
    lat, lon = get_coords_from_wikidata(qid)
    if lat is None or lon is None:
        place_str = parse_infobox_location(data["title"], lang)
        if place_str:
            coords = enhanced_geocoding(place_str)
            if coords:
                lat, lon = coords
    if lat is None or lon is None:
        lat, lon = 0.0, 0.0
    data["latitude"] = lat
    data["longitude"] = lon

    # (3) Enriquecimientos
    add_semantic_links(data)
    add_image_data(data)

    # (4) Fechas históricas
    data["historic_dates"] = extract_historic_dates(data["summary"])

    # (5) APA reference
    data["apa_reference"] = generate_apa_reference(data["title"], data["source_url"])

    # (6) Asignar prioridad
    assign_priority(data)

    # (7) Guardar en caché
    CacheManager.set_article(cache_key, data)
    return data

############################################################################
# ========= PROCESAR (LANG, KEYWORD) EN CONCURRENCIA =======================
############################################################################

def process_language_keyword(lang: str, keyword: str) -> List[Dict]:
    """
    Llama a 'advanced_search' para la keyword en 'lang' 
    y procesa cada artículo retornado en hilos.
    """
    logger.info(f"[{lang.upper()}] => {keyword}")
    results = advanced_search(keyword, lang)
    output = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futs = [executor.submit(process_article, art, keyword, lang) for art in results]
        for ft in as_completed(futs):
            try:
                out = ft.result()
                output.append(out)
            except Exception as e:
                logger.error(f"Error process_language_keyword: {e}")
    return output

############################################################################
# ====================== ANÁLISIS POST-PROCESO =============================
############################################################################

def generate_analytics(all_data: List[Dict]):
    """
    Módulo para analizar los datos recolectados.
    - Ejemplo: conteo por idioma, ver top 3 prioridad.
    """
    logger.info("=== Análisis Final ===")
    count_by_lang = {}
    for item in all_data:
        ln = item["lang"]
        count_by_lang[ln] = count_by_lang.get(ln, 0) + 1

    for k, v in count_by_lang.items():
        logger.info(f"  {k.upper()} => {v} artículos")

    # Ejemplo: top 3 por prioridad
    top3 = sorted(all_data, key=lambda x: x.get("priority", 0), reverse=True)[:3]
    logger.info("Top 3 por prioridad:")
    for t in top3:
        logger.info(f" * {t['title']} => priority={t['priority']}")

############################################################################
# ======================== FUNCIÓN PRINCIPAL ===============================
############################################################################

def main():
    t0 = time.time()
    all_articles = []

    # Concurrencia a nivel (lang, keyword)
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures_map = {}
        for lang in config.WIKI_LANGUAGES:
            for kw in KEYWORDS:
                fut = executor.submit(process_language_keyword, lang, kw)
                futures_map[fut] = (lang, kw)

        for fut in as_completed(futures_map):
            lang, keyword = futures_map[fut]
            try:
                chunk = fut.result()
                all_articles.extend(chunk)
            except Exception as e:
                logger.error(f"Fallo final en lang={lang}, kw={keyword}: {e}")

    # Analiza resultados
    generate_analytics(all_articles)

    # Guardar todo en "wikipedia_data.json"
    with open("wikipedia_data.json", "w", encoding="utf-8") as f:
        json.dump(all_articles, f, indent=2, ensure_ascii=False)

    dt = time.time() - t0
    logger.info(f"Proceso completado en {dt:.2f}s. Total artículos: {len(all_articles)}")

if __name__ == "__main__":
    main()
