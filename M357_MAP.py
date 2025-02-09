# -*- coding: utf-8 -*-
"""
M357_MAP.py - Sistema mejorado con geocodificación exhaustiva y detección de emociones
"""

import feedparser
import json
import logging
import os
import re
from typing import List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geojson import FeatureCollection, Feature, Point
from transformers import pipeline
import threading

############################################################################
# ============================ CONFIGURACIÓN ===============================
############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("masonic_alerts.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MasonicGeoAnalytics")

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
    "https://www.google.com/alerts/feeds/08823391955851607514/5792372986925203132",
    "https://www.google.com/alerts/feeds/08823391955851607514/8767673777731649427", 
    # Añade más feeds aquí si es necesario...
]

OUTPUT_FILE = "masonic_alerts.geojson"

GEOLOCATION_CONFIG = {
    'nominatim': {'user_agent': 'masonic_geo_v1', 'timeout': 15, 'rate_limit': 1.0}
}

LOCATION_REGEX = re.compile(
    r"\b(?:en\s+|in\s+|at\s+)([A-ZÀ-ÖØ-öø-ÿ][a-zÀ-ÖØ-öø-ÿ' -]+)",
    re.IGNORECASE | re.UNICODE
)

# Inicialización del modelo de emociones
emotion_classifier = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", return_all_scores=True)

############################################################################
# ==================== SISTEMA DE GEOCODIFICACIÓN ==========================
############################################################################

class GeoCache:
    """Caché simple para almacenar resultados de geocodificación y optimizar el rendimiento."""
    def __init__(self, max_size: int = 500):
        self.cache = {}
        self.max_size = max_size

    def get(self, key: str) -> Optional[Tuple[float, float]]:
        return self.cache.get(key)

    def set(self, key: str, value: Tuple[float, float]):
        if len(self.cache) >= self.max_size:
            self.cache.pop(next(iter(self.cache)))  # Eliminar el primer elemento (FIFO)
        self.cache[key] = value

geo_cache = GeoCache()

geolocator = Nominatim(
    user_agent=GEOLOCATION_CONFIG['nominatim']['user_agent'],
    timeout=GEOLOCATION_CONFIG['nominatim']['timeout']
)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=GEOLOCATION_CONFIG['nominatim']['rate_limit'])

def enhanced_geocode(location_text: str) -> Optional[Tuple[float, float]]:
    """Realiza la geocodificación utilizando un sistema de caché."""
    if not location_text:
        return None
    
    # Verificar en caché
    cached = geo_cache.get(location_text)
    if cached:
        return cached
    
    try:
        location = geolocator.geocode(location_text)
        if location and is_valid_coords(location.longitude, location.latitude):
            coords = (location.longitude, location.latitude)
            geo_cache.set(location_text, coords)
            return coords
    except Exception as e:
        logger.error(f"Error en geocodificación: {str(e)[:200]}")
    
    return None

def is_valid_coords(lon: float, lat: float) -> bool:
    """Verifica si las coordenadas están dentro de rangos válidos."""
    return -180 <= lon <= 180 and -90 <= lat <= 90

############################################################################
# ==================== FUNCIONES DE EXTRACCIÓN DE UBICACIONES ==============
############################################################################

def metadata_location(entry) -> Optional[Tuple[float, float]]:
    """Busca ubicación en los metadatos del feed."""
    if hasattr(entry, 'geo_lat') and hasattr(entry, 'geo_long'):
        try:
            lat = float(entry.geo_lat)
            lon = float(entry.geo_long)
            if is_valid_coords(lon, lat):
                return (lon, lat)
        except Exception:
            pass
    return None

def content_location(entry) -> Optional[Tuple[float, float]]:
    """Busca menciones de ubicaciones en el contenido del título y resumen."""
    clean_content = re.sub('<[^>]+>', '', f"{entry.title} {entry.summary}")
    for loc in LOCATION_REGEX.findall(clean_content):
        coords = enhanced_geocode(loc)
        if coords:
            return coords
    return None

############################################################################
# ==================== FUNCIONES PRINCIPALES ===============================
############################################################################

def detect_emotions(text: str) -> dict:
    """Detecta emociones en el texto utilizando NLP."""
    if not text.strip():
        return {"joy": 0, "sadness": 0, "surprise": 0, "fear": 0}
    
    emotions = emotion_classifier(text)
    if not emotions or len(emotions) == 0:
        return {"joy": 0, "sadness": 0, "surprise": 0, "fear": 0}

    emotion_scores = {emotion["label"].lower(): round(emotion["score"], 2) for emotion in emotions[0]}
    return {
        "joy": emotion_scores.get("joy", 0),
        "sadness": emotion_scores.get("sadness", 0),
        "surprise": emotion_scores.get("surprise", 0),
        "fear": emotion_scores.get("fear", 0)
    }

def process_feed_entry(entry) -> Optional[Feature]:
    """Procesa una entrada RSS y crea una Feature de GeoJSON."""
    try:
        title = entry.get('title', 'Sin título').strip()
        link = entry.get('link', '')
        published = entry.get('published', '')
        summary = re.sub('<[^>]+>', '', entry.get('summary', ''))

        # Detección de emociones
        emotions = detect_emotions(summary)

        # Estrategia de geocodificación múltiple
        coords = metadata_location(entry) or content_location(entry)

        properties = {
            'title': title,
            'link': link,
            'published': format_date(published),
            'description': summary[:500] + '...' if len(summary) > 500 else summary,
            'emotions': emotions,
            'author': entry.get('author', 'Desconocido'),
            'source': entry.get('source', {}).get('title', 'Fuente desconocida')
        }

        return Feature(geometry=Point(coords) if coords else None, properties=properties)
    except Exception as e:
        logger.error(f"Error procesando entrada: {str(e)[:200]}")
        return None

def format_date(date_str: str) -> str:
    """Formatea la fecha publicada en un formato legible."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return date_str[:19]

############################################################################
# ==================== FUSIÓN Y ALMACENAMIENTO =============================
############################################################################

def merge_geojson_data(new_data: FeatureCollection) -> None:
    existing_data = FeatureCollection([])

    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_data_json = json.load(f)
                valid_features = [
                    f for f in existing_data_json.get("features", [])
                    if isinstance(f, dict) and isinstance(f.get("properties", {}), dict)
                ]
                existing_data = FeatureCollection(valid_features)
        except Exception as e:
            logger.error(f"Error cargando datos existentes: {str(e)[:200]}")

    existing_ids = {
        f.get("properties", {}).get("link")
        for f in existing_data.get("features", [])
        if isinstance(f, dict) and isinstance(f.get("properties", {}), dict)
    }

    new_features = [
        f for f in new_data.get("features", [])
        if isinstance(f, dict) and f.get("properties", {}).get('link') not in existing_ids
    ]

    updated_features = existing_data.get("features", []) + new_features

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(FeatureCollection(updated_features), f, ensure_ascii=False, indent=2)

############################################################################
# ====================== EJECUCIÓN PRINCIPAL ===============================
############################################################################

def process_feed(feed_url: str) -> List[Feature]:
    response = requests.get(feed_url, timeout=15)
    response.raise_for_status()
    feed = feedparser.parse(response.content)
    return [entry for e in feed.entries if (entry := process_feed_entry(e))]

def main():
    logger.info("Iniciando recopilación de alertas masónicas")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_feed, url): url for url in RSS_FEEDS}
        results = []

        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                logger.error(f"Error en feed: {str(e)[:200]}")

    merge_geojson_data(FeatureCollection(results))
    logger.info(f"Proceso completado. Datos guardados en {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
