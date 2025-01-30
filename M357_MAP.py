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
from geojson import FeatureCollection, Feature, Point, dump
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
    # Pega aquí los enlaces de tus feeds RSS
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
    # ... (todos tus feeds)
]

# ============== CONSTANTES ==============
GEOCODING_ENABLED = True
MAX_SUMMARY_LENGTH = 200
MASTER_JSON = "master_data.json"
OUTPUT_GEOJSON = "new_data.geojson"
geolocator = Nominatim(user_agent="masoneria_geolocator_v2")
nlp = spacy.load("es_core_news_sm")

MASONIC_KEYWORDS = [
    "Masones", "Francmasonería", "Freemason", "Freimaurer", "Logia masónica", "Gran Logia",
    "Freemasonry", "Masonic Lodge", "Masonería", "Franc-maçon", "Franco-maçonaria", "Grande Loge",
    "Großloge", "Loge maçonnique", "Ordem maçônica", "Temple maçonnique", "Templo masónico"
]

# ============== FUNCIONES DE GEOCODIFICACIÓN ==============
@lru_cache(maxsize=500)
def geocode_location(location_str: str) -> tuple:
    try:
        time.sleep(1.2)  # Para evitar problemas con la API de geocoding
        loc = geolocator.geocode(location_str, exactly_one=True, timeout=15)
        return (loc.longitude, loc.latitude) if loc else (None, None)
    except Exception as e:
        logger.error(f"Geocoding error: {location_str} - {str(e)}")
        return (None, None)

def is_valid_coords(lon, lat) -> bool:
    if lon is None or lat is None:
        return False
    return -180 <= lon <= 180 and -90 <= lat <= 90

# ============== ANÁLISIS SEMÁNTICO ==============
def is_masonic_content(text: str) -> bool:
    doc = nlp(text)
    if any(keyword.lower() in text.lower() for keyword in MASONIC_KEYWORDS):
        return True
    for ent in doc.ents:
        if ent.label_ in ["ORG", "LOC"]:
            if any(keyword.lower() in ent.text.lower() for keyword in MASONIC_KEYWORDS):
                return True
    return False

# ============== FUNCIONES DE PROCESAMIENTO ==============
def extract_location(text: str) -> str:
    pattern = r"\b(?:in|en|at|de)\s+([A-ZÀ-ÿ][a-zA-ZÀ-ÿ\s-]+?)(?:\.|,|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.UNICODE)
    return match.group(1).strip() if match else None

def parse_feed(feed_url: str) -> list:
    try:
        feed = feedparser.parse(feed_url)
        if not feed.entries:
            logger.warning(f"El feed {feed_url} está vacío o tiene problemas.")
            return []

        entries = []
        for entry in feed.entries:
            combined_text = f"{entry.get('title', '')} {entry.get('summary', '')}"
            if not is_masonic_content(combined_text):
                continue

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
                location = extract_location(combined_text)
                if location:
                    lon, lat = geocode_location(location)
                    if is_valid_coords(lon, lat):
                        new_entry.update({"lon": lon, "lat": lat})

            entries.append(new_entry)
        
        return entries

    except Exception as e:
        logger.error(f"Error procesando feed: {feed_url} - {str(e)}")
        return []

# ============== FUNCIONES PRINCIPALES ==============
def load_master_data() -> list:
    if os.path.isfile(MASTER_JSON):
        with open(MASTER_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_master_data(data: list) -> None:
    with open(MASTER_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def is_duplicate(entry: dict, master_data: list) -> bool:
    return any(e["link"] == entry["link"] for e in master_data)

def generate_geojson(data: list) -> dict:
    features = []
    for item in data:
        if item["lon"] and item["lat"]:
            properties = {
                "title": item["title"],
                "description": f"{item['summary']}\n[[{item['link']}|Fuente]]",
                "published": item["published"],
                "image_url": item["image_url"],
                "link": item["link"]
            }
            features.append(Feature(geometry=Point((item["lon"], item["lat"])), properties=properties))
    return FeatureCollection(features)

def main():
    try:
        logger.info("Iniciando actualización de datos")
        master_data = load_master_data()
        new_entries = []

        for feed_url in RSS_FEEDS:
            entries = parse_feed(feed_url)
            new_entries.extend([e for e in entries if not is_duplicate(e, master_data)])
        
        if new_entries:
            logger.info(f"Nuevas entradas encontradas: {len(new_entries)}")
            with open(OUTPUT_GEOJSON, "w") as f:
                dump(generate_geojson(new_entries), f, ensure_ascii=False, indent=2)
            
            master_data.extend(new_entries)
            save_master_data(master_data)
            logger.info("Datos actualizados correctamente")
        else:
            logger.info("No hay nuevas entradas")
            
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}")
        raise

if __name__ == "__main__":
    main()
