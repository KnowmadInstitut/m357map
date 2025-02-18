import json
from geojson import Feature, FeatureCollection, Point, dump
from geopy.geocoders import Nominatim

# Configuración del geolocalizador (ajusta el user_agent según tu proyecto)
geolocator = Nominatim(user_agent="my_wiki_geocoder")

def load_wikipedia_data():
    """
    Carga el contenido de wikipedia_data.json y devuelve una lista de entradas.
    Si el JSON tiene la clave "features", se extrae esa lista.
    """
    with open("wikipedia_data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "features" in data:
        return data["features"]
    elif isinstance(data, list):
        return data
    else:
        raise ValueError("El formato de wikipedia_data.json no es el esperado.")

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
    Crea un FeatureCollection (GeoJSON) a partir de los datos.
    Usa lat/lon si están presentes, de lo contrario intenta geocodificar "location".
    Si "location" no existe, usa el "title" para intentar deducir la ubicación.
    Finalmente, utiliza (0,0) como fallback si no se obtiene una ubicación válida.
    """
    features = []
    for entry in data:
        # Verificar que la entrada es un diccionario
        if not isinstance(entry, dict):
            print(f"Entrada inválida (no es dict): {entry}")
            continue

        # Usar "summary" o, de no existir, "description" como respaldo
        summary = entry.get("summary", entry.get("description", ""))
        if not summary:
            print(f"Omitiendo entrada sin summary/description: {entry}")
            continue

        # Intentar obtener latitud y longitud directamente
        lat = entry.get("latitude")
        lon = entry.get("longitude")

        # Si no hay lat/lon válidos, intentar con geocodificación
        if not (isinstance(lat, (int, float)) and isinstance(lon, (int, float))):
            place_name = entry.get("location")
            # Si no se indica una ubicación, intentar usar el título como pista
            if not place_name:
                place_name = entry.get("title")
            lat, lon = geocode_location(place_name)

        # Si aún no se obtiene una ubicación válida, usar fallback (0,0)
        if not (isinstance(lat, (int, float)) and isinstance(lon, (int, float))):
            lat = 0.0
            lon = 0.0

        # GeoJSON requiere (longitud, latitud)
        geometry = Point((lon, lat))

        properties = {
            "title": entry.get("title", "Sin título"),
            "summary": summary,
            "source": "Wikipedia",
            "keyword": entry.get("keyword", ""),
            "raw_location": entry.get("location", "")
        }

        features.append(Feature(geometry=geometry, properties=properties))

    return FeatureCollection(features)

def main():
    try:
        data = load_wikipedia_data()
    except Exception as e:
        print(f"Error al cargar los datos: {e}")
        return

    fc = create_geojson_from_wikipedia(data)

    with open("wikipedia_data.geojson", "w", encoding="utf-8") as f:
        dump(fc, f, ensure_ascii=False, indent=2)

    print("✅ Generado wikipedia_data.geojson con uso de lat/lon, geocodificación y fallback.")

if __name__ == "__main__":
    main()
