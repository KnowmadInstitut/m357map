# -*- coding: utf-8 -*-
"""
masonic_google_alerts.py

Script avanzado para:
- Leer y procesar múltiples feeds de Google Alerts sobre masonería.
- Ejecutar en concurrencia con ThreadPoolExecutor.
- Generar y/o fusionar un archivo GeoJSON (google_alerts.geojson) sin borrar data previa.
- Validar el GeoJSON resultante.

Basado en el M357_MAP.py original, con optimizaciones inspiradas en el
'Wikipedia Scraper' approach.
"""

import feedparser
import json
import logging
import time
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

from geopy.geocoders import Nominatim
from geojson import FeatureCollection, Feature, Point, dumps, loads
# Si no tienes geojson, pip install geojson

############################################################################
# ============== CONFIGURACIÓN DEL LOG =====================================
############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("m357map.log"), logging.StreamHandler()]
)
logger = logging.getLogger("GoogleAlertsScraper")

############################################################################
# ============== LISTA DE FEEDS ============================================
############################################################################

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

OUTPUT_GEOJSON = "google_alerts.geojson"  # Cambia el nombre a tu gusto
geolocator = Nominatim(user_agent="masonic_alerts_app")

############################################################################
# ============= FUNCIONES DE VALIDACIÓN DE COORDENADAS =====================
############################################################################

def is_valid_coords(lon, lat):
    return (
        lon is not None and lat is not None and
        -180 <= lon <= 180 and -90 <= lat <= 90
    )

def validate_geojson_data(features_data: List[Dict]) -> FeatureCollection:
    """
    Convierte la lista de dicts en un FeatureCollection válido.
    Filtra cualquier feature con coords inválidas.
    """
    valid_features = []
    for fdict in features_data:
        geom = fdict.get("geometry")
        if not geom:
            continue
        coords = geom.get("coordinates", [])
        if len(coords) == 2 and is_valid_coords(coords[0], coords[1]):
            valid_features.append(fdict)
    if not valid_features:
        logger.warning("No se encontraron features válidas tras validación.")
        return FeatureCollection([])
    return FeatureCollection(valid_features)


############################################################################
# =========== FUSIÓN DE DATOS ANTIGUOS Y NUEVOS (SIN BORRAR HISTORIC) =====
############################################################################

def load_old_geojson(path: str) -> FeatureCollection:
    if not os.path.exists(path):
        return FeatureCollection([])
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
            return FeatureCollection(raw["features"])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return FeatureCollection([])

def merge_features(old_fc: FeatureCollection, new_fc: FeatureCollection) -> FeatureCollection:
    """
    Fusiona las features, usando 'link' como identificador único para
    actualizar entradas repetidas y mantener las nuevas.
    """
    merged_dict = {}

    # Convertir features antiguas en dict
    for feat in old_fc.features:
        props = feat.get("properties", {})
        link_id = props.get("link")  # unique
        if link_id:
            merged_dict[link_id] = feat
        else:
            # fallback con title
            merged_dict[props.get("title","unknown")] = feat

    # Actualizar con las nuevas
    for feat in new_fc.features:
        props = feat.get("properties", {})
        link_id = props.get("link")
        if link_id:
            merged_dict[link_id] = feat
        else:
            merged_dict[props.get("title","unknown")] = feat

    merged_features = list(merged_dict.values())
    return FeatureCollection(merged_features)

def save_merged_geojson(new_fc: FeatureCollection, path="google_alerts.geojson"):
    """
    Carga 'google_alerts.geojson' anterior, fusiona con new_fc,
    y reescribe el archivo.
    """
    old_fc = load_old_geojson(path)
    combined_fc = merge_features(old_fc, new_fc)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(combined_fc, f, ensure_ascii=False, indent=2)
    logger.info(f"Datos combinados guardados en {path}.")


############################################################################
# =============== PARSEAR UN FEED INDIVIDUAL ===============================
############################################################################

def parse_feed(feed_url: str) -> List[Feature]:
    """
    Lee un feed de Google Alerts con feedparser y genera una lista de Features GeoJSON,
    usando (None, None) como coords salvo que decidas extraer algo de la 'summary' (opcional).
    """
    feed = feedparser.parse(feed_url)
    if not feed.entries:
        logger.warning(f"El feed {feed_url} está vacío o sin entradas.")
        return []

    features = []
    for entry in feed.entries:
        # Asignar coords (en el M357_MAP.py original era simulado: lon=10.0, lat=20.0)
        # Podrías hacer un geocoding del summary, pero no es trivial.
        lon, lat = None, None

        # (Ejemplo) Si quisieras parsear "location" en 'summary', harías:
        # location_match = re.search(r"en la ciudad de (\w+)", entry["summary"].lower())
        # if location_match:
        #     city = location_match.group(1)
        #     # geocodificar 'city' => (lon, lat)...

        # Por defecto, dejamos coords en None => Se validará, si no hay coords => no se incl. o se pone 0,0
        # Si deseas forzar algo, p. ej:
        # lon, lat = 0.0, 0.0

        if lon is not None and lat is not None and is_valid_coords(lon, lat):
            geom = Point((lon, lat))
        else:
            # No coords => usaremos None para ver si lo filtras
            geom = Point((0.0, 0.0))  # si deseas fallback

        props = {
            "title": entry.get("title", "Sin título"),
            "summary": entry.get("summary", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", "")
            # podrías extraer "author" u otras fields si existen
        }

        feat = Feature(geometry=geom, properties=props)
        features.append(feat)

    return features


############################################################################
# ============== PROCESAR TODOS LOS FEEDS EN CONCURRENCIA ==================
############################################################################

def process_all_feeds() -> FeatureCollection:
    """
    1) Llama parse_feed en concurrent.futures para cada feed
    2) Retorna un FeatureCollection con la data
    """
    from concurrent.futures import ThreadPoolExecutor

    all_features = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        fut_map = {executor.submit(parse_feed, url): url for url in RSS_FEEDS}
        for fut in as_completed(fut_map):
            feed_url = fut_map[fut]
            try:
                feats = fut.result()
                all_features.extend(feats)
                logger.info(f"Procesado feed: {feed_url} => {len(feats)} entradas")
            except Exception as e:
                logger.error(f"Error procesando feed {feed_url}: {e}")

    return FeatureCollection(all_features)


############################################################################
# ============ FUNCIÓN PRINCIPAL ===========================================
############################################################################

def main():
    logger.info("Iniciando actualización de datos (Google Alerts)...")

    # 1) Parsear feeds en concurrencia
    new_fc = process_all_feeds()

    # 2) Validar y/o filtrar coords
    valid_fc = validate_geojson_data(new_fc.features)

    # 3) Fusionar con google_alerts.geojson (o el nombre que desees)
    save_merged_geojson(valid_fc, OUTPUT_GEOJSON)

    logger.info("Proceso finalizado con éxito.")


if __name__ == "__main__":
    main()
