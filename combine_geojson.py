import sys
import json

def combine_geojson(existing_file, new_file, output_file):
    # Cargar el archivo existente
    with open(existing_file, "r", encoding="utf-8") as f:
        existing_data = json.load(f)

    # Cargar el archivo nuevo
    with open(new_file, "r", encoding="utf-8") as f:
        new_data = json.load(f)

    # Combinar las características (features)
    combined_features = existing_data.get("features", []) + new_data.get("features", [])
    combined_data = {
        "type": "FeatureCollection",
        "features": combined_features
    }

    # Guardar el archivo combinado
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: combine_geojson.py <archivo_existente> <archivo_nuevo> <archivo_salida>")
        sys.exit(1)

    combine_geojson(sys.argv[1], sys.argv[2], sys.argv[3])
