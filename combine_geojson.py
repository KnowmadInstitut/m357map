import sys
import json
import logging
from geojson import FeatureCollection, load, dump

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def remove_duplicates(existing_features: list, new_features: list) -> list:
    """Elimina duplicados usando el campo 'link' como identificador único"""
    existing_links = {f["properties"]["link"] for f in existing_features if "link" in f["properties"]}
    return [f for f in new_features if f["properties"].get("link") not in existing_links]

def combine_geojson(existing_path: str, new_path: str, output_path: str) -> None:
    """Combina dos archivos GeoJSON preservando la integridad de los datos"""
    try:
        # Cargar datos con validación
        with open(existing_path, "r", encoding="utf-8") as f:
            existing = load(f)
        
        with open(new_path, "r", encoding="utf-8") as f:
            new = load(f)

        # Verificar estructura GeoJSON
        if not isinstance(existing, FeatureCollection) or not isinstance(new, FeatureCollection):
            raise ValueError("Archivos de entrada no son FeatureCollections válidos")

        # Filtrar duplicados
        unique_new = remove_duplicates(existing["features"], new["features"])
        
        # Crear archivo temporal
        temp_path = f"{output_path}.tmp"
        combined = FeatureCollection(existing["features"] + unique_new)
        
        with open(temp_path, "w", encoding="utf-8") as f:
            dump(combined, f, indent=2, ensure_ascii=False)

        # Reemplazar archivo original de forma segura
        import os
        os.replace(temp_path, output_path)
        
        logger.info(f"✅ Combinación exitosa: {len(unique_new)} nuevas entradas | Total: {len(combined['features'])}")

    except Exception as e:
        logger.error(f"🚨 Error crítico: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Uso: python combine_geojson.py <existente.geojson> <nuevo.geojson> <salida.geojson>")
        sys.exit(1)
        
    combine_geojson(sys.argv[1], sys.argv[2], sys.argv[3])
