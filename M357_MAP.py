# -*- coding: utf-8 -*-
"""M357_MAP.py"""
import feedparser  # Nombre correcto
import re
import json
import time
import os
import logging
from functools import lru_cache  # Corregido
from geopy.geocoders import Nominatim  # Importación correcta
from geojson import FeatureCollection, Feature, Point, dump

# Configuración de logging CORREGIDA
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("m357map.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuración principal
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
    "https://www.google.com/alerts/feeds/08823391955851607514/11098843941918965173"
]

GEOCODING_ENABLED = True
MAX_SUMMARY_LENGTH = 200
MASTER_JSON = "master_data.json"
OUTPUT_GEOJSON = "new_data.geojson"

# Configuración de geocodificación
geolocator = Nominatim(user_agent="masoneria_geolocator_v2")

@lru_cache(maxsize=500)
def geocode_location(location_str: str) -> tuple:
    """Geocodificación con caché y orden [longitud, latitud]"""
    try:
        time.sleep(1.2)  # Respeta política de uso de Nominatim
        loc = geolocator.geocode(location_str, exactly_one=True, timeout=15)
        return (loc.longitude, loc.latitude) if loc else (None, None)
    except Exception as e:
        logger.error(f"Error en geocodificación: {location_str} - {str(e)}")
        return (None, None)

def is_valid_coords(lon: float, lat: float) -> bool:
    """Valida coordenadas geográficas"""
    return (-180 <= lon <= 180) and (-90 <= lat <= 90)

def extract_location(text: str) -> str:
    """Extrae ubicaciones con expresión regular mejorada"""
    pattern = r"\b(?:in|en|at|de)\s+([A-ZÀ-ÿ][a-zA-ZÀ-ÿ\s-]+?)(?:\.|,|$)"
    match = re.search(pattern, text, re.IGNORECASE|re.UNICODE)
    return match.group(1).strip() if match else None

def parse_feed(feed_url: str) -> list:
    """Procesa feeds RSS con manejo robusto de errores"""
    try:
        feed = feedparser.parse(feed_url)
        entries = []
        
        for entry in feed.entries:
            new_entry = {
                "title": entry.get("title", "Sin título"),
                "summary": entry.get("summary", "")[:MAX_SUMMARY_LENGTH] + "..." if len(entry.get("summary", "")) > MAX_SUMMARY_LENGTH else entry.get("summary", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "image_url": next(iter(entry.get("media_content", [])), {}).get("url"),
                "lon": None,
                "lat": None
            }
            
            if GEOCODING_ENABLED:
                location = extract_location(f"{new_entry['title']} {new_entry['summary']}")
                if location:
                    lon, lat = geocode_location(location)
                    if is_valid_coords(lon, lat):
                        new_entry.update({"lon": lon, "lat": lat})
            
            entries.append(new_entry)
        
        return entries
    
    except Exception as e:
        logger.error(f"Error crítico procesando feed {feed_url}: {str(e)}")
        return []

# Resto de funciones (load_master_data, save_master_data, is_duplicate, generate_geojson, main) 
# se mantienen igual que en la versión anterior pero usando las variables de configuración locales

# ... (Las funciones restantes son idénticas a la versión anterior pero sin referencia a config.py)

if __name__ == "__main__":
    main()  # ✅ Llama a la función main()
