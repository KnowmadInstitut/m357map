# -*- coding: utf-8 -*-
"""
M357_MAP.py

Script Avanzado para recopilar Google Alerts sobre masonería:
1) Procesa múltiples feeds en concurrencia (ThreadPoolExecutor).
2) Genera campos como 'apa_citation', 'description', etc.
3) Detección avanzada de ubicación (al estilo 'DrugPolicyMap.py'):
   - Metadatos 'geo_lat', 'geo_long', 'location'
   - Campos adicionales (author, category, source)
   - Regex en title+summary
4) Geocodifica con Nominatim (RateLimiter) y fallback a Photon.
5) Fusiona datos nuevos con archivo previo google_alerts.geojson sin
   borrar información histórica.
"""

import feedparser
import json
import logging
import os
import re
from typing import List, Dict, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geojson import FeatureCollection, Feature, Point

############################################################################
# ============================ CONFIGURACIÓN ===============================
############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("m357map.log"), logging.StreamHandler()]
)
logger = logging.getLogger("GoogleAlertsScraper")

# Feeds
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
   ]

OUTPUT_FILE = "google_alerts.geojson"  # Nombre del archivo final

# Instancia geolocalizador con rate limiting
geolocator = Nominatim(user_agent="masonic_alerts_app")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)

############################################################################
# =================== DETECCIÓN DE UBICACIÓN AVANZADA ======================
############################################################################

def extract_possible_location(text: str) -> Optional[str]:
    """
    Busca patrones de estilo 'in Ciudad' o 'at Place' en un texto.
    Puedes ampliar este regex con tu lógica preferida.
    """
    # Ejemplo muy simple: busca 'in <Word>' o 'at <Word>'
    pattern = r"\b(?:in|at)\s+([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)*)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def extract_location_from_metadata(e) -> Optional[tuple]:
    """
    1. Revisa si la entrada tiene 'geo_lat' y 'geo_long'.
    2. O si tiene 'location'.
    """
    # 1) 'geo_lat' y 'geo_long'
    if hasattr(e, 'geo_lat') and hasattr(e, 'geo_long'):
        try:
            lat = float(e.geo_lat)
            lon = float(e.geo_long)
            return (lon, lat)  # geojson usa (lon, lat)
        except:
            pass

    # 2) 'location' en la entrada
    if hasattr(e, 'location'):
        location_str = getattr(e, 'location', '')
        coords = geocode_text(location_str)
        if coords != (None, None):
            return coords
    return None

def extract_location_from_additional_fields(e) -> Optional[tuple]:
    """
    Revisa campos como 'author', 'category', 'source' en busca de un 'location' textual
    y luego geocodifica.
    """
    fields = ['author', 'category', 'source']
    for field in fields:
        if hasattr(e, field):
            text = getattr(e, field, '')
            possible_loc = extract_possible_location(text)
            if possible_loc:
                coords = geocode_text(possible_loc)
                if coords != (None, None):
                    return coords
    return None

def advanced_extract_location(e) -> Optional[tuple]:
    """
    Intenta localizar la entrada usando diversos pasos, al estilo 'DrugPolicyMap.py'.
    1) Metadatos (geo_lat, geo_long, location)
    2) Campos extras (author, category, source)
    3) Regex en title + summary
    """
    # Paso 1: Revisar metadatos
    coords = extract_location_from_metadata(e)
    if coords:
        return coords

    # Paso 2: Revisar campos extra
    coords = extract_location_from_additional_fields(e)
    if coords:
        return coords

    # Paso 3: Regex en title + summary
    full_text = f"{getattr(e, 'title', '')} {getattr(e, 'summary', '')}"
    loc_str = extract_possible_location(full_text)
    if loc_str:
        coords = geocode_text(loc_str)
        if coords != (None, None):
            return coords

    # Si no se halló nada
    return None

############################################################################
# ================== GEOCOD / VAL COORDS ===================================
############################################################################

def is_valid_coords(lon, lat):
    return (
        lon is not None
        and lat is not None
        and -180 <= lon <= 180
        and -90 <= lat <= 90
    )

def geocode_text(location_text: str) -> tuple:
    """
    Llama a Nominatim con RateLimiter. Fallback a Photon.
    """
    if not location_text:
        return (None, None)
    try:
        loc = geocode(location_text)
        if loc:
            return (loc.longitude, loc.latitude)
        # fallback a Photon
        photon_url = f"https://photon.komoot.io/api/?q={location_text}"
        resp = requests.get(photon_url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            feats = data.get("features", [])
            if feats:
                coords = feats[0]["geometry"]["coordinates"]  # [lon, lat]
                return (coords[0], coords[1])
    except Exception as e:
        logger.warning(f"Error geocodificando '{location_text}': {e}")
    return (None, None)

############################################################################
# =============== GENERAR APA CITATION Y CAMPOS ============================
############################################################################

def generate_apa_citation(title: str, link: str, published: str) -> str:
    """
    Título. (YYYY-MM-DD). Retrieved from link
    """
    date_str = "n.d."
    if published:
        try:
            dt = datetime.strptime(published, "%Y-%m-%dT%H:%M:%SZ")
            date_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_str = published[:10]
    return f"{title}. ({date_str}). Retrieved from {link}"

############################################################################
# ============ MERGE DE GEOJSON (SIN BORRAR HISTÓRICO) =====================
############################################################################

from geojson import FeatureCollection, Feature, Point

def load_old_geojson(path: str) -> FeatureCollection:
    if not os.path.exists(path):
        return FeatureCollection([])
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return FeatureCollection(data["features"])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return FeatureCollection([])

def merge_features(old_fc: FeatureCollection, new_fc: FeatureCollection) -> FeatureCollection:
    """
    Fusión usando 'link' como ID principal.
    Si coincide, actualiza con la nueva.
    """
    merged_dict = {}
    # Convertir old
    for feat in old_fc.features:
        link_id = feat["properties"].get("link", "")
        merged_dict[link_id] = feat
    # Actualizar con new
    for feat in new_fc.features:
        link_id = feat["properties"].get("link", "")
        merged_dict[link_id] = feat
    return FeatureCollection(list(merged_dict.values()))

def save_merged_geojson(new_fc: FeatureCollection, path=OUTPUT_FILE):
    old_fc = load_old_geojson(path)
    combined = merge_features(old_fc, new_fc)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    logger.info(f"Datos combinados guardados en {path} con total={len(combined.features)} features.")

############################################################################
# =============== PARSEAR FEED + GENERAR FEATURE ===========================
############################################################################

def process_entry(entry) -> Feature:
    """
    Genera un Feature con:
      - apa_citation
      - description (summary)
      - link
      - published
      - title
      - geometry => coords reales si es detectado via advanced_extract_location
    """
    title = entry.get("title", "").strip()
    link = entry.get("link", "")
    published = entry.get("published", "")
    summary = entry.get("summary", "")

    # Generar APA
    apa = generate_apa_citation(title, link, published)

    # Detección avanzada de ubicación (al estilo "DrugPolicyMap")
    coords = advanced_extract_location(entry)

    # Construir 'properties'
    props = {
        "apa_citation": apa,
        "description": summary,
        "link": link,
        "published": published,
        "title": title
    }

    # Asignar geometry
    geometry = None
    if coords and is_valid_coords(coords[0], coords[1]):
        geometry = Point((coords[0], coords[1]))

    return Feature(geometry=geometry, properties=props)

def parse_feed(feed_url: str) -> List[Feature]:
    """
    Lee y procesa un feed con feedparser.
    Retorna una lista de Feature (geojson)
    """
    fd = feedparser.parse(feed_url)
    if not fd.entries:
        logger.warning(f"Feed vacío: {feed_url}")
        return []
    feats = []
    for e in fd.entries:
        feat = process_entry(e)
        feats.append(feat)
    return feats

############################################################################
# ========== PROCESAR TODOS LOS FEEDS EN CONCURRENCIA ======================
############################################################################

def process_all_feeds() -> FeatureCollection:
    """Procesa todos los feeds en concurrencia y retorna un FeatureCollection."""
    all_feats = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {executor.submit(parse_feed, url): url for url in RSS_FEEDS}
        for fut in as_completed(future_map):
            feed_url = future_map[fut]
            try:
                feats = fut.result()
                logger.info(f"{feed_url} => {len(feats)} entradas")
                all_feats.extend(feats)
            except Exception as e:
                logger.error(f"Error parseando feed {feed_url}: {e}")
    return FeatureCollection(all_feats)

############################################################################
# ============================ MAIN ========================================
############################################################################

def main():
    logger.info("Iniciando scraping de Google Alerts (Masonería) con detección avanzada de ubicación...")

    # 1) Procesar Feeds en concurrencia
    new_fc = process_all_feeds()
    
    # 2) Fusionar con el archivo previo
    save_merged_geojson(new_fc, OUTPUT_FILE)

    logger.info("Proceso completado. Revisa el archivo final (google_alerts.geojson) para ver la data unificada.")

if __name__ == "__main__":
    main()

