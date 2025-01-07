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
    # Lista completa de feeds
    "https://www.google.com/alerts/feeds/08823391955851607514/18357020651463187477",
    "https://www.google.com/alerts/feeds/08823391955851607514/434625937666013668",
    # ... añade los feeds restantes aquí
]

MASTER_JSON = "master_data.json"
OUTPUT_GEOJSON = "masoneria_alertas.geojson"

# Configuración para geocodificación
USE_GEOCODING = True
geolocator = Nominatim(user_agent="masoneria_geolocator")

# Funciones auxiliares
# ... (copia aquí las funciones como en tu script actual)

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

# Asegúrate de tener este bloque
if __name__ == "__main__":
    main()
