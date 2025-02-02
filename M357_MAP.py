# -*- coding: utf-8 -*-
"""M357_MAP.py"""
import feedparser
import json
import os
import logging
from geopy.geocoders import Nominatim
from geojson import FeatureCollection, Feature, Point, dumps, loads

# ============== CONFIGURACIÓN DEL LOG ==============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("m357map.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============== CONSTANTES ==============
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
OUTPUT_GEOJSON = "new_data.geojson"
MASTER_JSON = "master_data.json"
geolocator = Nominatim(user_agent="geojson_validator")

# ============== FUNCIONES ==============
def is_valid_coords(lon, lat):
    return lon is not None and lat is not None and -180 <= lon <= 180 and -90 <= lat <= 90

def validate_geojson(data):
    try:
        parsed_data = loads(dumps(data))
        logger.info("El GeoJSON es válido.")
        return parsed_data
    except Exception as e:
        logger.error(f"Error al validar GeoJSON: {str(e)}. Intentando reparación...")
        features = [feature for feature in data if "geometry" in feature and is_valid_coords(
            feature["geometry"]["coordinates"][0], feature["geometry"]["coordinates"][1]
        )]
        if features:
            logger.info(f"Se recuperaron {len(features)} características válidas.")
            return FeatureCollection(features)
        else:
            logger.error("No se pudieron recuperar características válidas.")
            return None

def save_geojson(data):
    validated_data = validate_geojson(data)
    if validated_data:
        with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
            f.write(dumps(validated_data, ensure_ascii=False, indent=2))
        logger.info(f"GeoJSON validado guardado en {OUTPUT_GEOJSON}.")
    else:
        logger.error("No se pudo guardar el archivo GeoJSON debido a errores.")

def parse_feed(feed_url):
    feed = feedparser.parse(feed_url)
    if not feed.entries:
        logger.warning(f"El feed {feed_url} está vacío.")
        return []

    entries = []
    for entry in feed.entries:
        lon, lat = None, None
        location = entry.get("location", "")
        # Simulación de coordenadas (GeoJSON usa orden [lon, lat])
        if location:
            lon, lat = 10.0, 20.0  

        if is_valid_coords(lon, lat):
            entries.append(Feature(
                geometry=Point((lon, lat)),  # GeoJSON: (longitud, latitud)
                properties={
                    "title": entry.get("title", "Sin título"),
                    "summary": entry.get("summary", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", "")
                }
            ))

    return entries

# ============== FUNCIONES PRINCIPALES ==============
def main():
    logger.info("Iniciando actualización de datos...")
    features = []

    for feed_url in RSS_FEEDS:
        features.extend(parse_feed(feed_url))

    if features:
        save_geojson(FeatureCollection(features))
    else:
        logger.warning("No se encontraron entradas válidas para guardar.")

if __name__ == "__main__":
    main()
