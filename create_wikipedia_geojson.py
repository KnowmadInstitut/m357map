# create_wikipedia_geojson.py

import json
from geojson import Feature, FeatureCollection, Point, dump

def load_wikipedia_data():
    """
    Carga el contenido del archivo wikipedia_data.json.
    Debe ser un JSON con estructura [{title, summary, categories, keyword, ...}, ...]
    """
    with open("wikipedia_data.json", "r", encoding="utf-8") as f:
        return json.load(f)

def create_geojson_from_wikipedia(data):
    """
    Genera un FeatureCollection en formato GeoJSON.
    Si no tienes coordenadas reales, se usarán (0, 0) a modo de ejemplo.
    """
    features = []
    for entry in data:
        # Validar que 'summary' exista y no esté vacío.
        if "summary" in entry and entry["summary"]:
            properties = {
                "title": entry["title"],
                "summary": entry["summary"],
                "categories": entry.get("categories", []),
                "source": "Wikipedia",
                "keyword": entry.get("keyword", "")
            }
            # Coordenadas ficticias (lon=0, lat=0)
            geometry = Point((0, 0))  
            features.append(Feature(geometry=geometry, properties=properties))

    # Construye el FeatureCollection y devuélvelo
    return FeatureCollection(features)

def main():
    # 1. Cargar datos de Wikipedia desde wikipedia_data.json
    wikipedia_data = load_wikipedia_data()

    # 2. Convertir a GeoJSON
    wikipedia_geojson = create_geojson_from_wikipedia(wikipedia_data)

    # 3. Guardar en un nuevo archivo (wikipedia_data.geojson)
    with open("wikipedia_data.geojson", "w", encoding="utf-8") as f:
        dump(wikipedia_geojson, f, ensure_ascii=False, indent=2)

    print("✅ Se generó wikipedia_data.geojson con éxito.")

if __name__ == "__main__":
    main()
