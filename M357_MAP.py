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
geolocator = Nominatim(user_agent="masoneria_geolocator_v2")
nlp = spacy.load("es_core_news_sm")

# ============== FUNCIONES AUXILIARES ==============
@lru_cache(maxsize=500)
def geocode_location(location_str: str) -> tuple:
    try:
        time.sleep(1.2)
        loc = geolocator.geocode(location_str, exactly_one=True, timeout=15)
        return (loc.longitude, loc.latitude) if loc else (None, None)
    except Exception as e:
        logger.error(f"Geocoding error: {location_str} - {str(e)}")
        return (None, None)

def is_valid_coords(lon: float, lat: float) -> bool:
    return lon is not None and lat is not None and (-180 <= lon <= 180) and (-90 <= lat <= 90)

def sanitize_json_string(input_str: str) -> str:
    """ Elimina caracteres problemáticos para evitar errores de JSON. """
    return input_str.replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()

def validate_geojson_structure(geojson_data) -> bool:
    """ Verifica si la estructura del GeoJSON es válida. """
    try:
        # Verificar que todas las entradas tengan las claves necesarias
        for feature in geojson_data.get("features", []):
            if "geometry" not in feature or "properties" not in feature:
                logger.error("Falta la clave 'geometry' o 'properties' en un elemento del GeoJSON.")
                return False

            if not is_valid_coords(
                feature.get("geometry", {}).get("coordinates", [None, None])[0],
                feature.get("geometry", {}).get("coordinates", [None, None])[1]
            ):
                logger.error("Coordenadas inválidas encontradas en el GeoJSON.")
                return False

        # Si todo es válido, devolvemos True
        return True
    except Exception as e:
        logger.error(f"Error al validar el GeoJSON: {str(e)}")
        return False

# ============== FUNCIONES DE PROCESAMIENTO DE FEEDS ==============
def parse_feed(feed_url: str) -> list:
    try:
        feed = feedparser.parse(feed_url)
        if not feed.entries:
            logger.warning(f"El feed {feed_url} está vacío o tiene problemas.")
            return []

        entries = []
        for entry in feed.entries:
            combined_text = f"{entry.get('title', '')} {entry.get('summary', '')}"
            if any(keyword.lower() in combined_text.lower() for keyword in ["masonería", "logia", "gran logia"]):
                # Procesar contenido masónico
                entries.append({
                    "title": sanitize_json_string(entry.get("title", "Sin título")),
                    "summary": sanitize_json_string(entry.get("summary", ""))[:MAX_SUMMARY_LENGTH] + "...",
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "lon": None,
                    "lat": None
                })

        return entries

    except Exception as e:
        logger.error(f"Error procesando feed: {feed_url} - {str(e)}")
        return []

# ============== MANEJO DE ARCHIVOS JSON Y GEOJSON ==============
def generate_geojson(data: list) -> dict:
    features = []
    for item in data:
        if item["lon"] and item["lat"]:
            features.append(Feature(
                geometry=Point((item["lon"], item["lat"])),
                properties={
                    "title": item["title"],
                    "description": item["summary"],
                    "link": item["link"],
                    "published": item["published"]
                }
            ))
    return FeatureCollection(features)

# ============== FUNCIÓN PRINCIPAL ==============
def main():
    logger.info("Iniciando actualización de datos")
    all_entries = []

    for feed_url in RSS_FEEDS:
        all_entries.extend(parse_feed(feed_url))

    if all_entries:
        logger.info(f"Nuevas entradas encontradas: {len(all_entries)}")
        geojson_data = generate_geojson(all_entries)

        # Validar estructura del GeoJSON antes de guardar
        if validate_geojson_structure(geojson_data):
            with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
                json.dump(geojson_data, f, ensure_ascii=False, indent=2)
            logger.info("GeoJSON generado correctamente.")
        else:
            logger.error("El archivo GeoJSON tiene una estructura inválida y no se generó.")
    else:
        logger.info("No hay nuevas entradas.")

if __name__ == "__main__":
    main()
