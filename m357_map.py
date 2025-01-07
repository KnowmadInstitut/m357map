# -*- coding: utf-8 -*-
"""M357_MAP.ipynb"""

import feedparser
import re
import json
import time
import os
from geopy.geocoders import Nominatim

# Configuración
RSS_FEEDS = [
    # Enlaces de feeds RSS
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
    "https://www.google.com/alerts/feeds/08823391955851607514/11937818240173291166"
]

MASTER_JSON = "master_data.json"
OUTPUT_GEOJSON = "masoneria_alertas.geojson"

USE_GEOCODING = True
geolocator = Nominatim(user_agent="masoneria_geolocator")

# Función para cargar datos del archivo maestro
def load_master_data():
    if os.path.isfile(MASTER_JSON):
        with open(MASTER_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

# Función para guardar datos en el archivo maestro
def save_master_data(data):
    with open(MASTER_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Verificar si la entrada ya existe
def is_duplicate(entry, master_data):
    link_new = entry.get("link", "")
    for item in master_data:
        if item.get("link", "") == link_new:
            return True
    return False

# Extraer posible ubicación de un texto
def extract_possible_location(text):
    pattern = r"\b(?:in|at)\s([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)*)"
    matches = re.findall(pattern, text)
    return matches[0] if matches else None

# Geocodificación usando Nominatim
def geocode_location(location_str):
    try:
        time.sleep(1)  # Respetar el límite de peticiones
        loc = geolocator.geocode(location_str)
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception as e:
        print(f"[ERROR] geocoding {location_str}: {e}")
    return None

# Parsear un feed RSS
def parse_feed(feed_url):
    feed = feedparser.parse(feed_url)
    entries = []

    for e in feed.entries:
        title = getattr(e, 'title', 'No Title')
        summary = getattr(e, 'summary', '')
        link = getattr(e, 'link', '')
        published = getattr(e, 'published', '')

        image_url = None
        if hasattr(e, 'media_content') and e.media_content:
            image_url = e.media_content[0].get('url')
        elif hasattr(e, 'media_thumbnail') and e.media_thumbnail:
            image_url = e.media_thumbnail[0].get('url')

        new_entry = {
            "title": title,
            "summary": summary,
            "link": link,
            "published": published,
            "image_url": image_url,
            "lat": None,
            "lon": None
        }

        if USE_GEOCODING:
            full_text = f"{title} {summary}"
            possible_location = extract_possible_location(full_text)
            if possible_location:
                coords = geocode_location(possible_location)
                if coords:
                    new_entry["lat"] = coords[0]
                    new_entry["lon"] = coords[1]

        entries.append(new_entry)

    return entries

# Generar datos en formato GeoJSON
def generate_geojson(data):
    geojson_data = {
        "type": "FeatureCollection",
        "features": []
    }

    for item in data:
        lat = item.get("lat")
        lon = item.get("lon")
        if lat is not None and lon is not None:
            summary = item.get("summary", "")
            if len(summary) > 200:
                summary = summary[:200] + "..."

            link = item.get("link", "")
            title = item.get("title", "No Title")
            published = item.get("published", "")
            image_url = item.get("image_url", None)

            description_text = f"{summary}\n\n[[{link}|Ver la fuente]]"

            feature = {
                "type": "Feature",
                "properties": {
                    "title": title,
                    "description": description_text,
                    "published": published,
                    "image_url": image_url
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                }
            }
            geojson_data["features"].append(feature)

    return geojson_data

# Script principal
def main():
    master_data = load_master_data()
    new_entries = []
    for feed_url in RSS_FEEDS:
        print(f"[INFO] Leyendo feed: {feed_url}")
        feed_entries = parse_feed(feed_url)
        for entry in feed_entries:
            if not is_duplicate(entry, master_data):
                new_entries.append(entry)

    if new_entries:
        print(f"[INFO] Nuevas entradas detectadas: {len(new_entries)}")
        new_geojson_data = generate_geojson(new_entries)
        with open("new_data.geojson", "w", encoding="utf-8") as f:
            json.dump(new_geojson_data, f, ensure_ascii=False, indent=2)
        print("[OK] Archivo 'new_data.geojson' generado.")

        master_data.extend(new_entries)
        save_master_data(master_data)
        print("[OK] master_data.json actualizado.")
    else:
        print("[INFO] No se encontraron nuevas entradas.")

if __name__ == "__main__":
    main()
