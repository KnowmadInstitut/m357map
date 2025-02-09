import os
import json
import requests
import sqlite3
import logging
import geojson
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import spacy

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("WikipediaMasonicScraper")

# ================== CONFIGURACIÓN ==================
WIKIPEDIA_LANGUAGES = ["en", "es", "fr", "de", "pt"]
CACHE_DB = "geo_cache.db"
JSON_OUTPUT = "wikipedia_masonic_data.json"
GEOJSON_OUTPUT = "wikipedia_masonic_data.geojson"
PARQUET_OUTPUT = "wikipedia_masonic_data.parquet"

SPACY_MODEL = "en_core_web_sm"

# Inicializar el modelo spaCy
try:
    nlp = spacy.load(SPACY_MODEL)
except OSError:
    logger.warning("Descargando e instalando el modelo spaCy...")
    import subprocess
    subprocess.run(["python", "-m", "spacy", "download", SPACY_MODEL])
    nlp = spacy.load(SPACY_MODEL)

# ================== CACHÉ EN SQLITE ==================
class GeoCache:
    def __init__(self):
        self.conn = sqlite3.connect(CACHE_DB)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                place TEXT PRIMARY KEY,
                lat REAL,
                lon REAL
            )
        """)
        self.conn.commit()

    def get(self, place: str) -> Optional[Tuple[float, float]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT lat, lon FROM locations WHERE place=?", (place,))
        row = cursor.fetchone()
        return (row[0], row[1]) if row else None

    def set(self, place: str, lat: float, lon: float):
        self.conn.execute("INSERT OR REPLACE INTO locations (place, lat, lon) VALUES (?, ?, ?)", (place, lat, lon))
        self.conn.commit()

geo_cache = GeoCache()

# ================== GEOCODIFICACIÓN HÍBRIDA ==================
geolocator = Nominatim(user_agent="wikipedia_masonic_scraper")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def geocode_location(location_text: str) -> Tuple[Optional[float], Optional[float]]:
    if not location_text:
        return None, None

    cached_coords = geo_cache.get(location_text)
    if cached_coords:
        return cached_coords

    try:
        location = geocode(location_text)
        if location:
            lat, lon = location.latitude, location.longitude
            geo_cache.set(location_text, lat, lon)
            return lat, lon
    except Exception as e:
        logger.warning(f"Error en geocodificación con Nominatim para '{location_text}': {e}")

    # Si falla Nominatim, intentar con Photon como fallback
    try:
        photon_url = f"https://photon.komoot.io/api/?q={location_text}"
        response = requests.get(photon_url)
        if response.status_code == 200:
            features = response.json().get("features", [])
            if features:
                lon, lat = features[0]["geometry"]["coordinates"]
                geo_cache.set(location_text, lat, lon)
                return lat, lon
    except Exception as e:
        logger.warning(f"Error en geocodificación con Photon para '{location_text}': {e}")

    return None, None

# ================== EXTRACCIÓN DE ENTIDADES ==================
def extract_location_spacy(text: str) -> Optional[str]:
    if not text:
        return None
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ == "GPE":  # Entidad geopolítica (ciudad, país)
            return ent.text
    return None

# ================== BÚSQUEDA Y PROCESAMIENTO ==================
def search_wikipedia(term: str, lang: str) -> List[Dict]:
    base_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": term,
        "srlimit": 50  # Máximo de resultados por consulta
    }
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json().get("query", {}).get("search", [])
    except Exception as e:
        logger.warning(f"Error al buscar en Wikipedia con '{term}' ({lang}): {e}")
    return []

def get_article_details(title: str, lang: str) -> Dict:
    base_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|coordinates|pageimages",
        "exintro": True,
        "explaintext": True,
        "titles": title,
        "pithumbsize": 500
    }
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            pages = response.json().get("query", {}).get("pages", {})
            for page in pages.values():
                return {
                    "title": page.get("title"),
                    "url": f"https://{lang}.wikipedia.org/wiki/{page.get('title').replace(' ', '_')}",
                    "description": page.get("extract", ""),
                    "coordinates": page.get("coordinates", [{}])[0],
                    "image": page.get("thumbnail", {}).get("source")
                }
    except Exception as e:
        logger.warning(f"Error al obtener detalles del artículo '{title}' ({lang}): {e}")
    return {}

def process_and_merge_entries(term: str, lang: str) -> List[Dict]:
    entries = search_wikipedia(term, lang)
    processed_entries = []

    for entry in entries:
        details = get_article_details(entry["title"], lang)
        coordinates = details.get("coordinates")
        lat, lon = geocode_location(extract_location_spacy(details["description"]))

        processed_entries.append({
            "title": details["title"],
            "url": details["url"],
            "description": details["description"],
            "coordinates": [lon, lat] if lon and lat else None,
            "image": details["image"],
            "language": lang,
            "timestamp": datetime.now().isoformat()
        })

    return processed_entries

# ================== GUARDAR RESULTADOS ==================
def save_results(results: List[Dict]):
    # JSON
    with open(JSON_OUTPUT, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # GeoJSON
    features = [
        geojson.Feature(
            geometry=geojson.Point((entry["coordinates"][0], entry["coordinates"][1])) if entry["coordinates"] else None,
            properties={key: value for key, value in entry.items() if key != "coordinates"}
        )
        for entry in results
    ]
    with open(GEOJSON_OUTPUT, "w") as f:
        geojson.dump(geojson.FeatureCollection(features), f, indent=2)

    # Parquet (opcional)
    try:
        pd.DataFrame(results).to_parquet(PARQUET_OUTPUT, index=False)
    except ImportError:
        logger.warning("pyarrow/parquet no está instalado. Saltando la exportación a Parquet.")

# ================== FUNCIÓN PRINCIPAL ==================
def main():
    combined_results = []

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_and_merge_entries, term, lang): (term, lang) for term in ["Freemasonry", "Gran Logia"] for lang in WIKIPEDIA_LANGUAGES}
        for future in as_completed(futures):
            try:
                combined_results.extend(future.result())
            except Exception as e:
                logger.error(f"Error en la ejecución del proceso: {e}")

    save_results(combined_results)
    logger.info(f"Proceso completado con {len(combined_results)} entradas.")

if __name__ == "__main__":
    main()
