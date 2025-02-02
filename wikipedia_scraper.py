# -*- coding: utf-8 -*-
"""
wikipedia_scraper.py

Script Avanzado para investigación masónica en Wikipedia:
- Gran lista de palabras clave multilingüe.
- Caché persistente en SQLite (o JSON si prefieres).
- Búsqueda avanzada con paginación (CirrusSearch).
- Geocodificación contextual gratuita (Photon + Nominatim).
- Rate limiting para evitar bloqueos de la API.
- Extracción de entidades (spaCy).
- Exportación a Parquet y GeoJSON para análisis.

Basado en código anterior con optimizaciones propuestas.
"""

import requests
import json
import logging
import time
import re
import os
import sqlite3
from typing import List, Dict, Optional, Tuple, Any
import mwparserfromhell
from datetime import datetime
from concurrent.futures import as_completed
import psutil

# Librerías extra
import dateparser
from tenacity import retry, stop_after_attempt, wait_exponential
from geopy.geocoders import Nominatim
from unidecode import unidecode

# Rate limiting (open source)
from ratelimit import limits, sleep_and_retry

# NLP con spaCy
import spacy

# Exportación a Parquet
import pandas as pd


############################################################################
# ============== CONFIGURACIÓN GLOBAL Y LOG ================================
############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(module)s] - %(message)s",
    handlers=[
        logging.FileHandler("masonic_research_optim.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MasonicWikiScraper")

class Config:
    # Idiomas de Wikipedia
    WIKI_LANGUAGES = ["en", "es"]
    # Timeout para peticiones
    REQUEST_TIMEOUT = 15
    # Usar SQLite para caché (o JSON)
    USE_SQLITE_CACHE = True
    SQLITE_DB = "scraper_cache.db"
    JSON_CACHE_FILE = "scraper_cache.json"

    # Regex para fechas históricas
    HISTORIC_DATES_REGEX = r"\b(\d{1,2}\s+\w+\s+\d{4}|\d{4})\b"
    # Retardo entre requests para no saturar APIs gratuitas
    REQUEST_DELAY = 1.0

config = Config()

# Inicializamos geolocalizador
geolocator = Nominatim(user_agent="masonic_research_v4")

# spaCy en inglés (modifica si usas otro idioma)
try:
    nlp = spacy.load("en_core_web_sm")  # O "es_core_news_sm" etc.
except Exception as e:
    logger.warning(f"spaCy model error: {e}")
    nlp = None

############################################################################
# ============= LISTA COMPLETA DE PALABRAS CLAVE MULTILINGÜES =============
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
    "Lessing et la Franc-maçonnerie allemande", "Royal Art", "Arte Real", "Art Royal", "Königliche Kunst"
]

############################################################################
# =========== RATE LIMITING (Por ejemplo para Wikipedia) ===================
############################################################################

WIKI_CALLS = 50
WIKI_PERIOD = 60  # 50 llamadas/min
@sleep_and_retry
@limits(calls=WIKI_CALLS, period=WIKI_PERIOD)
def call_wiki_api(*args, **kwargs):
    """
    Función genérica para llamar a requests.get con limitación de 50 req/min.
    """
    return requests.get(*args, **kwargs)

############################################################################
# ================ SISTEMA DE CACHÉ PERSISTENTE (SQLite o JSON) ============
############################################################################

def ensure_tables(conn: sqlite3.Connection):
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

class PersistentSQLiteCache:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        ensure_tables(self.conn)

    def get_article(self, cache_key: str) -> Optional[Dict]:
        c = self.conn.cursor()
        c.execute("SELECT data FROM articles WHERE cache_key = ?", (cache_key,))
        row = c.fetchone()
        if row:
            return json.loads(row[0])
        return None

    def set_article(self, cache_key: str, data: Dict):
        c = self.conn.cursor()
        c.execute("REPLACE INTO articles (cache_key, data) VALUES (?, ?)",
                  (cache_key, json.dumps(data, ensure_ascii=False)))
        self.conn.commit()

    def get_location(self, place_name: str) -> Optional[Tuple[float, float]]:
        c = self.conn.cursor()
        c.execute("SELECT lat, lon FROM locations WHERE place_name = ?", (place_name,))
        row = c.fetchone()
        if row:
            return (row[0], row[1])
        return None

    def set_location(self, place_name: str, lat: float, lon: float):
        c = self.conn.cursor()
        c.execute("REPLACE INTO locations (place_name, lat, lon) VALUES (?, ?, ?)",
                  (place_name, lat, lon))
        self.conn.commit()

    def close(self):
        self.conn.close()

def load_json_cache() -> Dict:
    if not os.path.exists(config.JSON_CACHE_FILE):
        return {"articles": {}, "locations": {}}
    try:
        with open(config.JSON_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"articles": {}, "locations": {}}

def save_json_cache(d: Dict):
    with open(config.JSON_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

class CacheManager:
    sqlite_cache: Optional[PersistentSQLiteCache] = None

    @staticmethod
    def init():
        if config.USE_SQLITE_CACHE:
            CacheManager.sqlite_cache = PersistentSQLiteCache(config.SQLITE_DB)

    @staticmethod
    def close():
        if CacheManager.sqlite_cache:
            CacheManager.sqlite_cache.close()

    @staticmethod
    def get_article(cache_key: str) -> Optional[Dict]:
        if config.USE_SQLITE_CACHE and CacheManager.sqlite_cache:
            return CacheManager.sqlite_cache.get_article(cache_key)
        else:
            data = load_json_cache()
            return data["articles"].get(cache_key)

    @staticmethod
    def set_article(cache_key: str, val: Dict):
        if config.USE_SQLITE_CACHE and CacheManager.sqlite_cache:
            CacheManager.sqlite_cache.set_article(cache_key, val)
        else:
            data = load_json_cache()
            data["articles"][cache_key] = val
            save_json_cache(data)

    @staticmethod
    def get_location(place_name: str) -> Optional[Tuple[float, float]]:
        if config.USE_SQLITE_CACHE and CacheManager.sqlite_cache:
            return CacheManager.sqlite_cache.get_location(place_name)
        else:
            data = load_json_cache()
            loc = data["locations"].get(place_name)
            if loc:
                return (loc["lat"], loc["lon"])
            return None

    @staticmethod
    def set_location(place_name: str, lat: float, lon: float):
        if config.USE_SQLITE_CACHE and CacheManager.sqlite_cache:
            CacheManager.sqlite_cache.set_location(place_name, lat, lon)
        else:
            d = load_json_cache()
            d["locations"][place_name] = {"lat": lat, "lon": lon}
            save_json_cache(d)

############################################################################
# =============== THREADPOOL DINÁMICO ======================================
############################################################################

from concurrent.futures import ThreadPoolExecutor

class DynamicExecutor(ThreadPoolExecutor):
    """
    Ajuste automático de workers.
    """
    def __init__(self):
        max_workers = min(32, (os.cpu_count() or 1) * 4)
        super().__init__(max_workers=max_workers)

############################################################################
# =============== BÚSQUEDA AVANZADA CON PAGINACIÓN =========================
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def advanced_search(keyword: str, lang: str, max_results=50) -> List[Dict]:
    base_url = f"https://{lang}.wikipedia.org/w/api.php"
    # Búsqueda CirrusSearch
    srsearch = f'"{keyword}" AND (incategory:"Masonic_buildings" OR hastemplate:"Infobox_freemasonry")'
    params = {
        "action": "query",
        "list": "search",
        "srsearch": srsearch,
        "format": "json",
        "srlimit": 50,
        "srprop": "size|wordcount|timestamp"
    }
    all_results = []
    sroffset = None

    while len(all_results) < max_results:
        time.sleep(config.REQUEST_DELAY)
        resp = call_wiki_api(base_url, params=params, timeout=config.REQUEST_TIMEOUT)
        data = resp.json()
        batch = data.get("query", {}).get("search", [])
        if not batch:
            break
        all_results.extend(batch)
        cont = data.get("continue")
        if cont and "sroffset" in cont:
            params["sroffset"] = cont["sroffset"]
        else:
            break

    return all_results[:max_results]

############################################################################
# ================== WIKIDATA (QID + P625) =================================
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def get_qid_from_wikipedia_page(page_title: str, lang: str) -> Optional[str]:
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "pageprops",
        "format": "json",
        "titles": page_title
    }
    rr = call_wiki_api(url, params=params, timeout=config.REQUEST_TIMEOUT)
    data = rr.json()
    pages = data["query"]["pages"]
    for _, page_info in pages.items():
        if "pageprops" in page_info and "wikibase_item" in page_info["pageprops"]:
            return page_info["pageprops"]["wikibase_item"]
    return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def get_coords_from_wikidata(qid: str) -> Tuple[Optional[float], Optional[float]]:
    if not qid:
        return None, None
    wd_url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    r = requests.get(wd_url, timeout=config.REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    entity = data["entities"].get(qid, {})
    claims = entity.get("claims", {})
    if "P625" in claims:
        coord_claim = claims["P625"][0]["mainsnak"]["datavalue"]["value"]
        lat = coord_claim["latitude"]
        lon = coord_claim["longitude"]
        return (lat, lon)
    return None, None

############################################################################
# ============== PARSE INFOBOX PARA UBICACIÓN ==============================
############################################################################

def parse_infobox_location(page_title: str, lang: str) -> Optional[str]:
    raw_url = f"https://{lang}.wikipedia.org/w/index.php?title={page_title}&action=raw"
    rr = call_wiki_api(raw_url, timeout=config.REQUEST_TIMEOUT)
    if rr.status_code != 200:
        return None
    wikitext = rr.text
    parsed = mwparserfromhell.parse(wikitext)
    templates = parsed.filter_templates()
    campos_ubicacion = ["birth_place","death_place","headquarters","location",
                        "place","foundation_place","venue","native_place"]
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
# =========== GEOCODIFICACIÓN CONTEXTUAL (PHOTON + NOMINATIM) ==============
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def enhanced_geocoding(place_name: str, context: str = "") -> Optional[Tuple[float, float]]:
    if not place_name:
        return None
    cached = CacheManager.get_location(place_name)
    if cached:
        return cached

    query = (place_name + " " + context).strip()
    query = unidecode(re.sub(r"\(.*?\)", "", query))

    services = [
        ("photon", "https://photon.komoot.io/api/"),
        ("nominatim", "https://nominatim.openstreetmap.org/search/")
    ]
    for svc, url in services:
        try:
            time.sleep(config.REQUEST_DELAY)
            if svc == "photon":
                resp = requests.get(url, params={"q": query}, timeout=10)
                j = resp.json()
                if j.get("features"):
                    coords = j["features"][0]["geometry"]["coordinates"]
                    lat, lon = coords[1], coords[0]
                    CacheManager.set_location(place_name, lat, lon)
                    return (lat, lon)
            elif svc == "nominatim":
                resp = requests.get(url, params={"q": query, "format":"json"}, timeout=10)
                j = resp.json()
                if j:
                    lat = float(j[0]["lat"])
                    lon = float(j[0]["lon"])
                    CacheManager.set_location(place_name, lat, lon)
                    return (lat, lon)
        except Exception as e:
            logger.warning(f"[{svc}] geocoding error '{query}': {e}")

    logger.error(f"No se pudo geocodificar: {place_name}")
    return None

############################################################################
# ============= FETCH DETALLES (EXTRACT + CATEGORIES) ======================
############################################################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def fetch_article_details(title: str, lang: str = "en") -> Dict:
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
    rr = call_wiki_api(endpoint, params=params, timeout=config.REQUEST_TIMEOUT)
    details = {"title": title, "summary": "", "categories": []}
    data = rr.json()
    pages = data.get("query", {}).get("pages", {})
    for _, pinfo in pages.items():
        details["title"] = pinfo.get("title", title)
        details["summary"] = pinfo.get("extract", "")
        cats = pinfo.get("categories", [])
        details["categories"] = [c.get("title","") for c in cats]
    return details

############################################################################
# ============== EXTRAER FECHAS HISTÓRICAS (DATEPARSER) ====================
############################################################################

def extract_historic_dates(text: str) -> List[Dict]:
    pattern = re.compile(config.HISTORIC_DATES_REGEX, re.IGNORECASE)
    matches = pattern.finditer(text)
    results = []
    for m in matches:
        raw_date = m.group(1)
        snippet = text[max(0,m.start()-30): m.end()+30]
        parsed = dateparser.parse(raw_date, settings={'PREFER_DAY_OF_MONTH':'first'})
        results.append({
            "raw": raw_date,
            "parsed": parsed.isoformat() if parsed else "",
            "context": snippet
        })
    return results

############################################################################
# ================ EXTRACCIÓN DE ENTIDADES (spaCy) =========================
############################################################################

def extract_entities_nlp(text: str, lang: str = "en") -> Dict[str, Any]:
    if not nlp:
        return {}
    doc = nlp(text)
    orgs = [ent.text for ent in doc.ents if ent.label_ == "ORG"]
    locations = [ent.text for ent in doc.ents if ent.label_ in ("GPE","LOC")]
    dates_ent = [ent.text for ent in doc.ents if ent.label_ == "DATE"]
    return {
        "organizations": orgs,
        "locations": locations,
        "dates_nlp": dates_ent
    }

############################################################################
# ============== APA REFERENCE =============================================
############################################################################

def generate_apa_reference(title: str, url: str) -> str:
    date_str = datetime.now().strftime("%d %B %Y")
    clean_title = title.replace("_", " ")
    return f"{clean_title}. (n.d.). En Wikipedia. Retrieved {date_str}, from {url}"

############################################################################
# =========== PRIORIDAD EN FUNCIÓN DE PALABRAS CLAVE EN TÍTULO ============
############################################################################

def assign_priority(article_data: Dict):
    t = article_data["title"].lower()
    p = 0
    if "grand lodge" in t:
        p += 50
    if "masonic temple" in t:
        p += 30
    article_data["priority"] = p

############################################################################
# =============== PIPELINE PRINCIPAL POR ARTÍCULO ==========================
############################################################################

def process_article(article: Dict, keyword: str, lang: str) -> Dict:
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

    # 1) Detalles (extract + categories)
    det = fetch_article_details(article["title"], lang=lang)
    data["title"] = det.get("title", data["title"])
    data["summary"] = det.get("summary", "")
    data["categories"] = det.get("categories", [])

    # 2) QID + coords (Wikidata)
    qid = get_qid_from_wikipedia_page(data["title"], lang)
    lat, lon = get_coords_from_wikidata(qid)

    # 3) Infobox -> geocoding
    if lat is None or lon is None:
        place_str = parse_infobox_location(data["title"], lang)
        if place_str:
            coords = enhanced_geocoding(place_str)
            if coords:
                lat, lon = coords

    # >>>>> NUEVO PASO: usar NLP si seguimos sin coords <<<<<
    if (lat is None or lon is None) and nlp:
        # Extraer entidades NLP
        nlp_ents = extract_entities_nlp(data["summary"], lang=lang)
        # Guardar en data, para no perder la info
        data["nlp_entities"] = nlp_ents

        possible_locs = nlp_ents.get("locations", [])
        if possible_locs:
            # Tomar la primera ubicación
            city_candidate = possible_locs[0]
            coords = enhanced_geocoding(city_candidate)
            if coords:
                lat, lon = coords

    # Fallback final
    if lat is None or lon is None:
        lat, lon = 0.0, 0.0

    data["latitude"] = lat
    data["longitude"] = lon

    # 4) Fechas históricas
    data["historic_dates"] = extract_historic_dates(data["summary"])

    # (Si no habías guardado NLP antes, hazlo aquí; pero ya lo hicimos arriba)
    if "nlp_entities" not in data:
        data["nlp_entities"] = extract_entities_nlp(data["summary"], lang=lang)

    # 5) APA reference
    data["apa_reference"] = generate_apa_reference(data["title"], data["source_url"])

    # 6) Prioridad
    assign_priority(data)

    # 7) Guardar en caché
    CacheManager.set_article(cache_key, data)
    return data

############################################################################
# =========== PROCESAR (LANG, KEYWORD) CONCURRENTE =========================
############################################################################

def process_language_keyword(lang: str, keyword: str) -> List[Dict]:
    logger.info(f"[{lang.upper()}] => {keyword}")
    results = advanced_search(keyword, lang, max_results=50)
    output = []
    with DynamicExecutor() as executor:
        fut_map = {executor.submit(process_article, art, keyword, lang): art for art in results}
        for fut in as_completed(fut_map):
            try:
                out = fut.result()
                output.append(out)
            except Exception as e:
                logger.error(f"process_article error: {e}")
    return output

############################################################################
# =============== ANÁLISIS & EXPORTACIÓN (PARQUET / GEOJSON) ===============
############################################################################

def generate_analytics(all_data: List[Dict]):
    logger.info("=== Análisis Final ===")

    # Conteo por idioma
    count_by_lang = {}
    for item in all_data:
        ln = item["lang"]
        count_by_lang[ln] = count_by_lang.get(ln, 0) + 1
    for k, v in count_by_lang.items():
        logger.info(f"[{k.upper()}] => {v} artículos")

    # Top 3 por prioridad
    top3 = sorted(all_data, key=lambda x: x.get("priority", 0), reverse=True)[:3]
    logger.info("Top 3 por prioridad:")
    for t in top3:
        logger.info(f" * {t['title']} => priority={t['priority']}")

def export_analysis_data(data: List[Dict]):
    """
    Exporta en Parquet y GeoJSON para análisis posterior.
    """
    df = pd.DataFrame(data)
    df.to_parquet("masonic_data.parquet", index=False)
    logger.info("Datos guardados en masonic_data.parquet")

    # Construir GeoJSON
    features = []
    for d in data:
        lat = d.get("latitude", 0.0)
        lon = d.get("longitude", 0.0)
        if lat != 0.0 or lon != 0.0:
            feat = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": d
            }
            features.append(feat)
    geojson = {"type":"FeatureCollection","features":features}
    with open("masonic_map.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    logger.info("Mapa guardado en masonic_map.geojson")

############################################################################
# ============================= MAIN =======================================
############################################################################

def main():
    t0 = time.time()
    # Iniciar caché persistente (SQLite o JSON)
    CacheManager.init()

    all_data = []
    with DynamicExecutor() as exec_lang:
        future_map = {}
        for lang in config.WIKI_LANGUAGES:
            for kw in KEYWORDS:
                fut = exec_lang.submit(process_language_keyword, lang, kw)
                future_map[fut] = (lang, kw)

        for fut in as_completed(future_map):
            lang, keyword = future_map[fut]
            try:
                chunk = fut.result()
                all_data.extend(chunk)
            except Exception as e:
                logger.error(f"Error final con {lang.upper()}-{keyword}: {e}")

    # Análisis final
    generate_analytics(all_data)
    export_analysis_data(all_data)

    # Cerrar caché
    CacheManager.close()

    dt = time.time() - t0
    logger.info(f"Proceso completado en {dt:.2f}s con {len(all_data)} artículos.")


if __name__ == "__main__":
    main()
