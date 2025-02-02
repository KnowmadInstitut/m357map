# -*- coding: utf-8 -*-
"""
M357_MAP.py

Script Avanzado para recopilar Google Alerts sobre masonería:
1) Procesa múltiples feeds en concurrencia (ThreadPoolExecutor).
2) Genera campos como 'apa_citation', 'description', etc., manteniendo
   la estructura anterior.
3) Geocodifica texto con Nominatim + Photon (opcional), omitiendo puntos
   sin coordenadas válidas.
4) Fusiona datos nuevos con archivo previo google_alerts.geojson sin
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

from geopy.geocoders import Nominatim
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

# Lista de Feeds
RSS_FEEDS = [
    # Aquí tus URLs:

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

# Instancia geolocalizador
geolocator = Nominatim(user_agent="masonic_alerts_app")
# Rate limit (p.ej., 1 seg entre peticiones para evitar bloqueo)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)

############################################################################
# ================== FUNCIONES DE GEOCODIFICACIÓN ==========================
############################################################################

def geocode_text(location_text: str):
    """
    Geocodifica un texto usando geopy (Nominatim).
    Si falla, fallback a Photon.
    Retorna (lon, lat) o (None, None).
    """
    if not location_text:
        return (None, None)
    try:
        # Primero Nominatim
        loc = geocode(location_text)
        if loc:
            return (loc.longitude, loc.latitude)
        # Fallback a Photon
        photon_url = f"https://photon.komoot.io/api/?q={location_text}"
        resp = requests.get(photon_url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            features = data.get("features", [])
            if features:
                coords = features[0]["geometry"]["coordinates"]  # [lon, lat]
                return (coords[0], coords[1])
    except Exception as e:
        logger.warning(f"Error geocodificando '{location_text}': {e}")
    return (None, None)

def is_valid_coords(lon, lat):
    return (
        lon is not None
        and lat is not None
        and -180 <= lon <= 180
        and -90 <= lat <= 90
    )

############################################################################
# =============== GENERAR APA CITATION Y CAMPOS ============================
############################################################################

def generate_apa_citation(title: str, link: str, published: str) -> str:
    """
    Genera un string APA 7 simplificado:
    Título. (YYYY-MM-DD). Retrieved from link
    """
    date_str = "n.d."
    if published:
        try:
            # published = '2025-02-01T07:31:04Z'
            dt = datetime.strptime(published, "%Y-%m-%dT%H:%M:%SZ")
            date_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_str = published[:10]
    return f"{title}. ({date_str}). Retrieved from {link}"

############################################################################
# ============= MERGE DE GEOJSON (SIN BORRAR HISTÓRICO) ====================
############################################################################

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
# ================== PARSEAR FEED + GENERAR FEATURE ========================
############################################################################

def process_entry(entry) -> Feature:
    """
    Genera un Feature con:
      - apa_citation
      - description (summary)
      - link
      - published
      - title
      - location (si se desea)
      - geometry => coords reales si se geocodifica un 'location', sino None
    """
    title = entry.get("title", "").strip()
    link = entry.get("link", "")
    published = entry.get("published", "")
    summary = entry.get("summary", "")
    
    # APA
    apa = generate_apa_citation(title, link, published)
    
    # (Opcional) Extraer 'location' desde entry o summary
    # No todos los Google Alerts proveen 'location' = entry["location"].
    # Podrías extraerlo con un regex de summary si deseas
    location_text = entry.get("location", "")
    # Si no hay location en entry, intenta un regex en summary
    # e.g.: location_text = extract_location_from_summary(summary)

    # Geocodificar
    lon, lat = None, None
    if location_text:
        lon, lat = geocode_text(location_text)
    
    # Campos 'properties'
    props = {
        "apa_citation": apa,
        "description": summary,
        "link": link,
        "published": published,
        "title": title,
        "location": location_text
    }
    
    # Si coords no son válidas, geometry => None
    if is_valid_coords(lon, lat):
        geometry = Point((lon, lat))
    else:
        geometry = None
    
    return Feature(geometry=geometry, properties=props)

def parse_feed(feed_url: str) -> List[Feature]:
    """Lee y procesa un feed. Retorna Features list."""
    import feedparser
    feed_data = feedparser.parse(feed_url)
    if not feed_data.entries:
        logger.warning(f"Feed vacío: {feed_url}")
        return []
    
    feats = []
    for e in feed_data.entries:
        feat = process_entry(e)
        feats.append(feat)
    return feats

############################################################################
# =========== PROCESAR TODOS LOS FEEDS EN CONCURRENCIA =====================
############################################################################

def process_all_feeds() -> FeatureCollection:
    """Procesa todos los feeds en concurrencia y retorna un FeatureCollection."""
    from concurrent.futures import ThreadPoolExecutor
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
# ========================== MAIN ==========================================
############################################################################

def main():
    logger.info("Iniciando scraping de Google Alerts (Masonería)...")

    # 1) Procesar Feeds en concurrencia
    new_fc = process_all_feeds()
    
    # 2) Fusionar con el archivo previo
    save_merged_geojson(new_fc, OUTPUT_FILE)

    logger.info("Proceso completado. Revisa el archivo final para ver la data unificada.")

if __name__ == "__main__":
    main()
