# -*- coding: utf-8 -*-
"""M357_MAP.py"""
import feedparser
import re
import json
import time
import os
import logging
from functools import lru_cache
from geopy.geocoders import Nominatim
from geojson import FeatureCollection, Feature, Point
import spacy

# ============== CONFIGURACIÓN DEL LOG ==============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("m357map.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============== INSERTA TUS URL DE RSS AQUÍ ==============
RSS_FEEDS = [

    "https://www.google.com/alerts/feeds/08823391955851607514/18357020651463187477",
    "https://www.google.com/alerts/feeds/08823391955851607514/434625937666013668",
    "https://www.google.com/alerts/feeds/08823391955851607514/303056625914324165",
    "https://www.google.com/alerts/feeds/08823391955851607514/9378709536916495456",
    "https://www.google.com/alerts/feeds/08823391955851607514/17243776362555978691",
    "https://www.google.com/alerts/feeds/08823391955851607514/15847044508852608532",
    "https://www.google.com/alerts/feeds/08823391955851607514/6833079353494005014",
    "https://www.google.com/alerts/feeds/08823391955851607514/5572981003473119348",
    "https://www.google.com/alerts/feeds/08823391955851607514/17383807687186980718",
    "https://www.google.com/alerts/feeds/08823391955851607514/11043471059141282309",
    "https://www.google.com/alerts/feeds/08823391955851607514/13877290848809114470",
    "https://www.google.com/alerts/feeds/08823391955851607514/10413993926495102043",
    "https://www.google.com/alerts/feeds/08823391955851607514/2031900511198117844",
    "https://www.google.com/alerts/feeds/08823391955851607514/16568355059505461850",
    "https://www.google.com/alerts/feeds/08823391955851607514/16568355059505461178",
    "https://www.google.com/alerts/feeds/08823391955851607514/7760122889210870690",
    "https://www.google.com/alerts/feeds/08823391955851607514/15183025294765855574",
    "https://www.google.com/alerts/feeds/08823391955851607514/4297759070181606765",
    "https://www.google.com/alerts/feeds/08823391955851607514/11630540178333861502",
    "https://www.google.com/alerts/feeds/08823391955851607514/15251611368669093385",
    "https://www.google.com/alerts/feeds/08823391955851607514/9684782093161547179",
    "https://www.google.com/alerts/feeds/08823391955851607514/8744244600052796540",
    "https://www.google.com/alerts/feeds/08823391955851607514/357094683772830109",
    "https://www.google.com/alerts/feeds/08823391955851607514/13155130439785831467",
    "https://www.google.com/alerts/feeds/08823391955851607514/15809012670835506226",
    "https://www.google.com/alerts/feeds/08823391955851607514/14458568452294133843",
    "https://www.google.com/alerts/feeds/08823391955851607514/3528049070088672707",
    "https://www.google.com/alerts/feeds/08823391955851607514/11937818240173291166",
    "https://www.google.com/alerts/feeds/08823391955851607514/11098843941918965173",
    # ... Agrega más feeds aquí si es necesario
]

# ============== CONSTANTES ==============
GEOCODING_ENABLED = True
MAX_SUMMARY_LENGTH = 200
MASTER_JSON = "master_data.json"
OUTPUT_GEOJSON = "new_data.geojson"
geolocator = Nominatim(user_agent="masonic_geo_locator_v3")
nlp = spacy.load("es_core_news_sm")

# ============== FUNCIONES DE GEOCODIFICACIÓN ==============
@lru_cache(maxsize=500)
def geocode_location(location_str):
    try:
        time.sleep(1.2)
        loc = geolocator.geocode(location_str, exactly_one=True, timeout=15)
        return (loc.longitude, loc.latitude) if loc else (None, None)
    except Exception as e:
        logger.error(f"Error de geocodificación en {location_str}: {str(e)}")
        return (None, None)

def is_valid_coords(lon, lat):
    return lon is not None and lat is not None and (-180 <= lon <= 180) and (-90 <= lat <= 90)

# ============== FUNCIONES DE PROCESAMIENTO ==============
def parse_feed(feed_url):
    try:
        feed = feedparser.parse(feed_url)
        if not feed.entries:
            logger.warning(f"El feed {feed_url} está vacío o tiene problemas.")
            return []

        entries = []
        for entry in feed.entries:
            combined_text = f"{entry.get('title', '')} {entry.get('summary', '')}"

            new_entry = {
                "title": entry.get("title", "Sin título"),
                "summary": entry.get("summary", "")[:MAX_SUMMARY_LENGTH] + "..." if len(entry.get("summary", "")) > MAX_SUMMARY_LENGTH else entry.get("summary", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "lon": None,
                "lat": None
            }

            if GEOCODING_ENABLED:
                location = extract_location(combined_text)
                if location:
                    lon, lat = geocode_location(location)
                    if is_valid_coords(lon, lat):
                        new_entry.update({"lon": lon, "lat": lat})

            entries.append(new_entry)
        return entries

    except Exception as e:
        logger.error(f"Error procesando feed {feed_url}: {str(e)}")
        return []

def extract_location(text):
    """Extrae posibles ubicaciones de un texto usando expresiones regulares."""
    pattern = r"\b(?:in|en|at|de)\s+([A-ZÀ-ÿ][a-zA-ZÀ-ÿ\s-]+?)(?:\.|,|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.UNICODE)
    return match.group(1).strip() if match else None

def validate_and_write_geojson(data, output_file=OUTPUT_GEOJSON):
    try:
        # Validar si el JSON es válido antes de guardarlo
        json_data = FeatureCollection(data)
        json_str = json.dumps(json_data, ensure_ascii=False, indent=2)

        # Intentar cargar el JSON para validar
        json.loads(json_str)  # Esto lanzará una excepción si el JSON está mal formado

        # Escribir el JSON al archivo si es válido
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(json_str)
            logger.info(f"Archivo GeoJSON validado y guardado en {output_file}.")
    except Exception as e:
        logger.error(f"Error validando o guardando GeoJSON: {str(e)}")

# ============== FUNCIONES PRINCIPALES ==============
def main():
    logger.info("Iniciando actualización de datos...")
    master_data = []

    # Leer datos anteriores si existen
    if os.path.isfile(MASTER_JSON):
        with open(MASTER_JSON, "r", encoding="utf-8") as f:
            master_data = json.load(f)

    new_entries = []

    # Procesar feeds RSS
    for feed_url in RSS_FEEDS:
        entries = parse_feed(feed_url)
        new_entries.extend([e for e in entries if e not in master_data])

    if new_entries:
        logger.info(f"Nuevas entradas encontradas: {len(new_entries)}")

        # Construcción del archivo GeoJSON
        features = []
        for entry in new_entries:
            if is_valid_coords(entry["lon"], entry["lat"]):
                features.append(Feature(
                    geometry=Point((entry["lon"], entry["lat"])),
                    properties={
                        "title": entry["title"],
                        "description": entry["summary"],
                        "link": entry["link"],
                        "published": entry["published"]
                    }
                ))

        # Validar y guardar solo si hay características válidas
        if features:
            validate_and_write_geojson(features)
            master_data.extend(new_entries)

            # Guardar el nuevo archivo de datos principales
            with open(MASTER_JSON, "w", encoding="utf-8") as f:
                json.dump(master_data, f, ensure_ascii=False, indent=2)
        else:
            logger.warning("No se encontraron características válidas para guardar en el GeoJSON.")
    else:
        logger.info("No hay nuevas entradas.")

if __name__ == "__main__":
    main()
