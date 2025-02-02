# -*- coding: utf-8 -*-
"""wikipedia_scraper.py

Combina un motor de búsqueda avanzado en Wikipedia con enriquecimiento de datos 
(Wikidata, Infobox, geocodificación, APA references, etc.) y concurrencia.
"""

import requests
import json
import logging
import time
from typing import List, Dict, Optional, Tuple
import mwparserfromhell
from geopy.geocoders import Nominatim
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from unidecode import unidecode
import pytz
import re

# ============== CONFIGURACIÓN AVANZADA DE LOG =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(module)s] - %(message)s",
    handlers=[
        logging.FileHandler("masonic_research.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MasonicWikiScraper")

# ============== CONFIGURACIÓN DINÁMICA =================
class Config:
    # Idiomas de Wikipedia a recorrer
    WIKI_LANGUAGES = ["en", "es", "fr", "de"]  # puedes ampliar a "pt", "it", etc.
    MAX_WORKERS = 5
    REQUEST_TIMEOUT = 15
    CACHE_FILE = "scraper_cache.json"

    # Regex que intenta capturar fechas históricas (ejemplo simplificado)
    # Ajusta según tus necesidades idiomáticas:
    HISTORIC_DATES_REGEX = r"\b(\d{1,2}\s+(?:de\s+)?[A-Za-z]+\s+(?:de\s+)?\d{4}|\d{4})\b"

    # Tiempo de pausa entre peticiones para no saturar APIs
    REQUEST_DELAY = 1.5

    # API key de MapQuest (ejemplo si deseas usar su geocodificación)
    MAPQUEST_API_KEY = "YOUR_API_KEY_HERE"  # <-- reempázalo o déjalo vacío

config = Config()

# Geolocalizador base
geolocator = Nominatim(user_agent="masonic_research_v2")

# ============== LISTA DE PALABRAS CLAVE (COMBINADA) =================
# Si deseas reusar tu anterior KEYWORDS, pégalas aquí.
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

    # ... Resto de tu lista ampliada
]

# ============== BUSQUEDA SEMÁNTICA / FILTRADA =================
def semantic_search(keyword: str, lang: str) -> List[Dict]:
    """
    Búsqueda semántica en Wikipedia con filtrado adicional "masonic"|"freemason"|"lodge"|"grand lodge"
    para refinar los resultados y extraer los más relevantes.
    """
    params = {
        "action": "query",
        "list": "search",
        "srsearch": f'{keyword} "masonic"|"freemason"|"lodge"|"grand lodge"',
        "format": "json",
        "srlimit": 10,
        "srprop": "size|wordcount|timestamp"
    }
    try:
        response = requests.get(
            f"https://{lang}.wikipedia.org/w/api.php",
            params=params,
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()
        results = response.json().get("query", {}).get("search", [])
        # Ordenar por wordcount y tomar los 5 más extensos
        return sorted(results, key=lambda x: x.get('wordcount', 0), reverse=True)[:5]

    except requests.exceptions.RequestException as e:
        logger.error(f"Búsqueda fallida para {keyword} ({lang}): {str(e)}")
        return []

# ============== SISTEMA DE CACHÉ =================
class CacheManager:
    @staticmethod
    def load() -> Dict:
        try:
            with open(config.CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"articles": {}, "locations": {}}

    @staticmethod
    def save(cache: Dict):
        with open(config.CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)

# ============== OBTENER QID / COORDENADAS WIKIDATA =================
def get_qid_from_wikipedia_page(page_title: str, lang: str = "en") -> Optional[str]:
    """
    Retorna el QID de un artículo de Wikipedia, p.ej. 'Q12345'.
    """
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "pageprops",
        "format": "json",
        "titles": page_title
    }
    try:
        r = requests.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
        data = r.json()
        pages = data["query"]["pages"]
        for _, page_info in pages.items():
            if "pageprops" in page_info and "wikibase_item" in page_info["pageprops"]:
                return page_info["pageprops"]["wikibase_item"]
    except Exception as e:
        logger.warning(f"No se pudo obtener QID para {page_title}: {e}")
    return None

def get_coords_from_wikidata(qid: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Retorna (lat, lon) de la propiedad P625 en Wikidata, o (None, None) si no existe.
    """
    if not qid:
        return None, None
    try:
        url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
        r = requests.get(url, timeout=config.REQUEST_TIMEOUT)
        data = r.json()
        entity = data["entities"].get(qid, {})
        claims = entity.get("claims", {})
        if "P625" in claims:
            coord_claim = claims["P625"][0]["mainsnak"]["datavalue"]["value"]
            lat = coord_claim["latitude"]
            lon = coord_claim["longitude"]
            return (lat, lon)
    except Exception as e:
        logger.warning(f"Error obteniendo coords de Wikidata para {qid}: {e}")
    return None, None

# ============== PARSE INFBOX LOCATION =================
def parse_infobox_location(page_title: str, lang: str = "en") -> Optional[str]:
    """
    Descarga el wikitext raw de la página y busca campos típicos de ubicación.
    """
    url_raw = f"https://{lang}.wikipedia.org/w/index.php?title={page_title}&action=raw"
    try:
        response = requests.get(url_raw, timeout=config.REQUEST_TIMEOUT)
        if response.status_code != 200:
            return None
        wikitext = response.text
    except Exception as e:
        logger.warning(f"Error obteniendo wikitext de {page_title}: {e}")
        return None

    parsed = mwparserfromhell.parse(wikitext)
    templates = parsed.filter_templates()

    campos_ubicacion = [
        "birth_place", "death_place", "headquarters", "location",
        "place", "foundation_place", "venue", "native_place"
    ]
    for template in templates:
        name = template.name.strip().lower()
        if "infobox" in name:
            for campo in campos_ubicacion:
                if template.has(campo):
                    val = template.get(campo).value.strip()
                    if val:
                        return str(val)
    return None

# ============== GEOCODIFICACIÓN MEJORADA =================
def enhanced_geocoding(place_name: str) -> Optional[Tuple[float, float]]:
    """
    1) Verifica caché
    2) Limpia el 'place_name'
    3) Intenta Photon -> Nominatim -> MapQuest
    4) Retorna la primera coincidencia
    """
    if not place_name:
        return None

    cache = CacheManager.load()
    if place_name in cache["locations"]:
        latlon = cache["locations"][place_name]
        return (latlon["lat"], latlon["lon"])

    # limpiar paréntesis, normalizar, etc.
    clean_place = re.sub(r"\(.*?\)", "", place_name).strip()
    clean_place = unidecode(clean_place)

    # Distintos servicios
    services = [
        ("photon", "https://photon.komoot.io/api/"),
        ("nominatim", "https://nominatim.openstreetmap.org/search/"),
        ("mapquest", "https://www.mapquestapi.com/geocoding/v1/address")
    ]

    for service, url in services:
        try:
            params = {}
            if service == "photon":
                params = {"q": clean_place}
                resp = requests.get(url, params=params, timeout=10)
                data = resp.json()
                if "features" in data and len(data["features"]) > 0:
                    best = data["features"][0]
                    coords = best["geometry"]["coordinates"]
                    lat, lon = coords[1], coords[0]
                    # Guardar en caché
                    cache["locations"][place_name] = {"lat": lat, "lon": lon}
                    CacheManager.save(cache)
                    return (lat, lon)

            elif service == "nominatim":
                params = {"q": clean_place, "format": "json"}
                resp = requests.get(url, params=params, timeout=10)
                data = resp.json()
                if len(data) > 0:
                    best = data[0]
                    lat = float(best["lat"])
                    lon = float(best["lon"])
                    cache["locations"][place_name] = {"lat": lat, "lon": lon}
                    CacheManager.save(cache)
                    return (lat, lon)

            elif service == "mapquest" and config.MAPQUEST_API_KEY:
                params = {"key": config.MAPQUEST_API_KEY, "location": clean_place}
                resp = requests.get(url, params=params, timeout=10)
                data = resp.json()
                if "results" in data and len(data["results"]) > 0:
                    loc = data["results"][0]["locations"]
                    if loc and len(loc) > 0:
                        lat = loc[0]["latLng"]["lat"]
                        lon = loc[0]["latLng"]["lng"]
                        cache["locations"][place_name] = {"lat": lat, "lon": lon}
                        CacheManager.save(cache)
                        return (lat, lon)

        except Exception as e:
            logger.warning(f"Error en {service} para {clean_place}: {e}")

    logger.error(f"Geocodificación fallida para: {clean_place}")
    return None

# ============== FUNCIÓN PARA OBTENER EXTRACT Y CATEGORÍAS =================
def fetch_article_details(title: str, lang="en") -> Dict:
    """
    Retorna un dict con {title, summary, categories}.
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
    details = {"title": title, "summary": "", "categories": []}
    try:
        response = requests.get(endpoint, params=params, timeout=config.REQUEST_TIMEOUT)
        if response.status_code == 200:
            pages = response.json().get("query", {}).get("pages", {})
            for _, page_info in pages.items():
                details["title"] = page_info.get("title", title)
                details["summary"] = page_info.get("extract", "")
                cats = page_info.get("categories", [])
                details["categories"] = [cat.get("title", "") for cat in cats]
    except Exception as e:
        logger.warning(f"Error fetching details de {title} ({lang}): {e}")
    return details

# ============== EXTRAER FECHAS HISTÓRICAS =================
def extract_historic_dates(text: str) -> List[Dict]:
    """
    Usa un regex para extraer fechas (formato flexible) y contextualizarlas.
    """
    pattern = re.compile(config.HISTORIC_DATES_REGEX, re.IGNORECASE)
    matches = list(pattern.finditer(text))
    results = []

    for match in matches:
        raw_date = match.group(1)
        snippet = text[max(0, match.start()-40): match.end()+40]
        # Intentar parsear
        parsed_date = None
        try:
            # Ejemplo: "2 de Enero de 1885"
            # Ajusta a tus idiomas
            # O intenta solo año
            if re.match(r"^\d{4}$", raw_date):
                parsed_date = datetime.strptime(raw_date, "%Y")
            else:
                # Podrías necesitar un parse manual
                pass
        except Exception:
            pass

        results.append({
            "raw": raw_date,
            "context": snippet,
            "parsed": parsed_date.isoformat() if parsed_date else ""
        })
    return results

# ============== AGREGAR ENLACES RELACIONADOS E IMÁGENES =================
def add_semantic_links(article_data: Dict):
    """
    Usa la API de Wikimedia (core/v1) para encontrar enlaces 
    que incluyan 'masonic' en su título.
    """
    lang = article_data.get("lang", "en")
    try:
        # API actual (experimental) --> Podría variar si la wiki no la soporta
        resp = requests.get(
            f"https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{article_data['title']}/links",
            timeout=config.REQUEST_TIMEOUT
        )
        js = resp.json()
        related = []
        for pg in js.get("pages", []):
            t = pg["title"].lower()
            if "masonic" in t or "freemason" in t:
                related.append(pg["title"])
        article_data["related_links"] = related
    except Exception as e:
        logger.warning(f"Error al obtener enlaces relacionados: {e}")

def add_image_data(article_data: Dict):
    """
    Obtiene imágenes con la palabra 'lodge' o 'masonic' en el título.
    """
    lang = article_data.get("lang", "en")
    params = {
        "action": "query",
        "prop": "images",
        "titles": article_data["title"],
        "format": "json"
    }
    try:
        resp = requests.get(f"https://{lang}.wikipedia.org/w/api.php", params=params, timeout=config.REQUEST_TIMEOUT)
        data = resp.json()
        pages = data["query"]["pages"]
        images_list = []
        for v in pages.values():
            imgs = v.get("images", [])
            for img in imgs:
                low = img["title"].lower()
                if "lodge" in low or "masonic" in low:
                    images_list.append(img["title"])
        article_data["images"] = images_list
    except Exception as e:
        logger.warning(f"Error obteniendo imágenes: {e}")

# ============== APA REFERENCE =================
def generate_apa_reference(title: str, url: str) -> str:
    """
    Genera referencia APA 7 simplificada para Wikipedia.
    Ejemplo: 
    Título. (n.d.). En Wikipedia. Retrieved 24 September 2025, from <URL>
    """
    date_str = datetime.now().strftime("%d %B %Y")
    cleaned_title = title.replace("_", " ")
    return f"{cleaned_title}. (n.d.). En Wikipedia. Retrieved {date_str}, from {url}"

# ============== COORDENADAS COMPLETAS (WIKIDATA + INFOBOX + GEOCOD) =================
def get_coordinates(page_title: str, lang: str) -> Tuple[float, float]:
    """
    1) Busca QID -> coords en Wikidata
    2) Si no, parsea Infobox para 'location' y geocodifica
    3) Fallback (0,0) si nada
    """
    # 1. QID
    qid = get_qid_from_wikipedia_page(page_title, lang)
    lat_wiki, lon_wiki = get_coords_from_wikidata(qid)
    if lat_wiki is not None and lon_wiki is not None:
        return (lat_wiki, lon_wiki)

    # 2. Parse infobox
    place_str = parse_infobox_location(page_title, lang)
    if place_str:
        coords = enhanced_geocoding(place_str)
        if coords:
            return coords[0], coords[1]

    # 3. Fallback
    return (0.0, 0.0)

# ============== PIPELINE PRINCIPAL POR ARTÍCULO =================
def process_article(article: Dict, keyword: str, lang: str) -> Dict:
    """
    Procesa un artículo de Wikipedia:
      - Extrae detalles (extract, categories)
      - Coord. (wikidata + infobox + geocoding)
      - Enlaces relacionados, imágenes
      - Fechas históricas, referencia APA
      - Maneja caché
    """
    cache = CacheManager.load()
    cache_key = f"{lang}_{article['title']}"
    if cache_key in cache["articles"]:
        # Ya procesado
        return cache["articles"][cache_key]

    # --- Preparar campos básicos ---
    article_data = {
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

    # 1. Detalles (extract, categories)
    details = fetch_article_details(article["title"], lang=lang)
    article_data["summary"] = details.get("summary", "")
    article_data["categories"] = details.get("categories", [])
    real_title = details.get("title", article["title"])  # por si el título oficial difiere
    article_data["title"] = real_title

    # 2. Coordenadas
    lat, lon = get_coordinates(real_title, lang)
    article_data["latitude"] = lat
    article_data["longitude"] = lon

    # 3. Enriquecimiento
    add_semantic_links(article_data)
    add_image_data(article_data)

    # 4. Fechas históricas
    article_data["historic_dates"] = extract_historic_dates(article_data["summary"])

    # 5. APA reference
    article_data["apa_reference"] = generate_apa_reference(real_title, article_data["source_url"])

    # 6. Guardar en caché
    cache["articles"][cache_key] = article_data
    CacheManager.save(cache)

    return article_data

# ============== PROCESAR UN PAR (LANG, KEYWORD) =================
def process_language_keyword(lang: str, keyword: str) -> List[Dict]:
    """
    Realiza la búsqueda semántica para la palabra clave, 
    y procesa cada artículo retornado.
    """
    logger.info(f"Iniciando procesamiento para [{lang.upper()}] {keyword}")
    articles = semantic_search(keyword, lang)
    results = []

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_article, art, keyword, lang) for art in articles]

        for fut in as_completed(futures):
            try:
                result = fut.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error procesando artículo: {str(e)}")

    return results

# ============== FUNCIÓN PARA ANALÍTICA (PUEDE SER PERSONALIZADA) =================
def generate_analytics(data: List[Dict]):
    """
    Aquí podrías realizar distintos análisis:
      - Conteo de artículos por idioma
      - Ubicación geográfica y densidad
      - Fechas históricas más recurrentes
    Por ahora, se deja como placeholder.
    """
    # Ejemplo de conteo simple
    logger.info("== Análisis Simple ==")
    by_lang = {}
    for d in data:
        by_lang.setdefault(d["lang"], 0)
        by_lang[d["lang"]] += 1
    for k, v in by_lang.items():
        logger.info(f"  [{k.upper()}] => {v} artículos")

# ============== FUNCIÓN PRINCIPAL =================
def main():
    start_time = time.time()
    all_data = []

    # Concurrencia a nivel de (idioma, palabra clave)
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_map = {}
        for lang in Config.WIKI_LANGUAGES:
            for keyword in KEYWORDS:
                fut = executor.submit(process_language_keyword, lang, keyword)
                future_map[fut] = (lang, keyword)

        for fut in as_completed(future_map):
            lang, kw = future_map[fut]
            try:
                lang_data = fut.result()
                all_data.extend(lang_data)
            except Exception as e:
                logger.error(f"Error procesando {lang.upper()} - {kw}: {str(e)}")

    # Analítica o post-proceso
    generate_analytics(all_data)

    # Guardar en un JSON final
    with open("wikipedia_data.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start_time
    logger.info(f"Proceso completado en {elapsed:.2f} seg. Total de artículos: {len(all_data)}")

if __name__ == "__main__":
    main()
