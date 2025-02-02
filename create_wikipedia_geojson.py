# create_wikipedia_geojson.py

import json
from geojson import Feature, FeatureCollection, Point, dump
from geopy.geocoders import Nominatim

# Prepara un geolocalizador (para "location" strings)
# Ajusta "user_agent" a algo representativo de tu proyecto
geolocator = Nominatim(user_agent="my_wiki_geocoder")

def load_wikipedia_data():
    """
    Carga el contenido de wikipedia_data.json y lo devuelve como lista/dict.
    """
    with open("wikipedia_data.json", "r", encoding="utf-8") as f:
        return json.load(f)

def geocode_location(place_name):
    """
    Intenta geocodificar una ubicación usando geopy (Nominatim).
    Retorna (lat, lon) o (None, None) si falla.
    """
    if not place_name:
        return None, None
    try:
        location = geolocator.geocode(place_name, timeout=10)
        if location:
            return (location.latitude, location.longitude)
    except Exception as e:
        print(f"Error geocodificando '{place_name}': {e}")
    return None, None

def create_geojson_from_wikipedia(data):
    """
    Crea un FeatureCollection (GeoJSON).
    Usa lat/lon si están presentes, de lo contrario intenta geocodificar "location".
    Finalmente, fallback a (0,0) si todo falla.
    """
    features = []
    for entry in data:
        summary = entry.get("summary", "")
        if not summary:
            # Si no hay summary o es vacío, omite esta entrada
            continue

        # 1. Chequear si hay lat/lon
        lat = entry.get("latitude")
        lon = entry.get("longitude")

        # 2. Si no existen lat/lon válidos, intenta con geocodificación
        if not (isinstance(lat, (int, float)) and isinstance(lon, (int, float))):
            place_name = entry.get("location")
            lat, lon = geocode_location(place_name)

        # 3. Si no se obtuvo nada, fallback (0,0)
        if not (isinstance(lat, (int, float)) and isinstance(lon, (int, float))):
            lat = 0.0
            lon = 0.0

        # GeoJSON usa (lon, lat)
        geometry = Point((lon, lat))

        # Construir las propiedades
        properties = {
            "title": entry.get("title", "Sin título"),
            "summary": summary,
            "source": "Wikipedia",
            "keyword": entry.get("keyword", ""),
            # Guardamos info extra como "raw_location" si lo deseas
            "raw_location": entry.get("location", "")
        }

        # Agregar el feature
        features.append(Feature(geometry=geometry, properties=properties))

    return FeatureCollection(features)

def main():
    # 1. Cargar los datos desde wikipedia_data.json
    data = load_wikipedia_data()

    # 2. Crear FeatureCollection con la lógica combinada
    fc = create_geojson_from_wikipedia(data)

    # 3. Guardar en archivo 'wikipedia_data.geojson'
    with open("wikipedia_data.geojson", "w", encoding="utf-8") as f:
        dump(fc, f, ensure_ascii=False, indent=2)

    print("✅ Generado wikipedia_data.geojson con uso de lat/lon y geocodificación si es necesario.")

if __name__ == "__main__":
    main()
