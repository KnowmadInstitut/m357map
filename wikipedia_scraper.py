import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from datetime import datetime

# Configuración inicial
geolocator = Nominatim(user_agent="m357_map_v1", timeout=20)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

BATCH_SIZE = 100  # Entradas por lote
PROGRESS_FILE = "progress.txt"
WIKIPEDIA_JSON = "wikipedia_data.json"
GEOJSON_OUTPUT = "wikipedia_data.geojson"

# Términos relevantes de búsqueda (en varios idiomas)
SEARCH_TERMS = [
  "Freemason", "Mason", "Francmason", "Freemasonry", "Francmasonería", "Gran Logia", "Masonic Lodge",
  "Masonic Temple", "Loge maçonnique", "Freimaurer", "Freimaurerei", "Franc-maçon", "Masonic Order",
  "Grand Orient", "Ancient and Accepted Scottish Rite", "Rito Escocés Antiguo y Aceptado", "York Rite",
  "Rito de York", "Knights Templar", "Caballeros Templarios", "Chevaliers du Temple", "Cavaleiros Templários",
  "Quatuor Coronati", "Hiram Abiff", "Anderson's Constitutions", "Constituciones de Anderson",
  "Constitutions d’Anderson", "Operative Masonry", "Maçonnerie Opérative", "Operative Maurerei",
  "Maçonnerie Opérative", "Free-Masons", "Franc-Maçons", "Franc-Masones", "Estatutos de Schaw",
  "Schaw Statutes", "Schaw-Statuten", "Lodge of Antiquity", "Logia de la Antigüedad", "Loge de l’Antiquité",
  "Mother Kilwinning", "Mãe Kilwinning", "Mère Kilwinning", "Entered Apprentice", "Aprendiz Masón",
  "Apprenti Maçon", "Lehrling", "Templar Freemasonry", "Masonería Templaria", "Maçonnerie Templière",
  "Templer-Freimaurerei", "Regius Manuscript", "Manuscrito Regius", "Manuscrit Regius", "Egregor",
  "Égrégore", "Royal Arch Masonry", "Real Arco Masónico", "Arco Real Maçônico", "Arc Royal Maçonnique",
  "Schottische Grade", "Grados del Rito Escocés", "Graus do Rito Escocês", "Degrés du Rite Écossais",
  "Brotherhood of Light", "Hermandad de la Luz", "Irmandade da Luz", "Fraternité de la Lumière",
  "Symbolic Masonry", "Masonería Simbólica", "Maçonnerie Symbolique", "Symbolische Maurerei",
  "Gothic Cathedral and Masonry", "Catedral Gótica y Masonería", "Cathédrale Gothique et Maçonnerie",
  "Gotische Kathedralen und Freimaurerei", "Speculative Masonry", "Masonería Especulativa",
  "Maçonnerie Spéculative", "Spekulative Maurerei", "Latin American Freemasonry", "Masonería en América Latina",
  "Maçonnerie en Amérique Latine", "Maçonaria na América Latina", "Landmarks of Freemasonry",
  "Landmarks Masónicos", "Landmarks der Freimaurerei", "Landmarks Maçônicos", "Landmarks Maçonniques",
  "Grand Orient of France", "Gran Oriente de Francia", "Grand Orient de France", "Grande Oriente da França",
  "Rectified Scottish Rite", "Rito Escocés Rectificado", "Rite Écossais Rectifié", "Rito Escocês Retificado",
  "Wilhelmsbad Convention", "Convención de Wilhelmsbad", "Convenção de Wilhelmsbad", "Convention de Wilhelmsbad",
  "Ramsay's Oration", "Oración de Ramsay", "Oração de Ramsay", "Discours de Ramsay", "Ramsays Rede",
  "Lessing and German Freemasonry", "Lessing y la Masonería Alemana", "Lessing e a Maçonaria Alemã",
  "Lessing et la Franc-maçonnerie allemande", "Royal Art", "Arte Real", "Arte Real", "Art Royal", "Königliche Kunst"
    # Agregar más términos si es necesario
]

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def save_progress(current_index):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(current_index))

def search_wikipedia(term, lang="en"):
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": term,
        "srlimit": 50
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("query", {}).get("search", [])
    except requests.RequestException as e:
        print(f"Error en la búsqueda de Wikipedia para '{term}': {e}")
        return []

def get_article_details(title, lang="en"):
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|coordinates|pageimages",
        "exintro": True,
        "explaintext": True,
        "titles": title,
        "pithumbsize": 500
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        pages = response.json().get("query", {}).get("pages", {})
        for page_id, page in pages.items():
            return {
                "title": page.get("title"),
                "url": f"https://{lang}.wikipedia.org/wiki/{page.get('title').replace(' ', '_')}",
                "description": page.get("extract", ""),
                "coordinates": page.get("coordinates", [{}])[0],
                "image": page.get("thumbnail", {}).get("source")
            }
    except requests.RequestException as e:
        print(f"Error al obtener detalles del artículo '{title}': {e}")
        return {}

def geocode_location(coordinates):
    if not coordinates:
        return None
    try:
        lat, lon = coordinates.get("lat"), coordinates.get("lon")
        if lat and lon:
            return [lon, lat]  # GeoJSON requiere [longitud, latitud]
    except KeyError:
        pass
    return None

def process_entries(entries, lang="en"):
    results = []
    for entry in entries:
        details = get_article_details(entry["title"], lang)
        coordinates = geocode_location(details.get("coordinates"))
        if details:
            results.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point" if coordinates else None,
                    "coordinates": coordinates if coordinates else []
                },
                "properties": {
                    "title": details["title"],
                    "url": details.get("url", "N/A"),
                    "description": details["description"],
                    "image": details["image"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "language": lang
                }
            })
    return results

def merge_and_save_geojson(new_features):
    existing_data = []
    if os.path.exists(GEOJSON_OUTPUT):
        try:
            with open(GEOJSON_OUTPUT, "r") as f:
                existing_data = json.load(f).get("features", [])
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error al cargar el archivo GeoJSON existente: {e}")

    # Eliminar duplicados basados en URL
    existing_urls = {feature.get("properties", {}).get("url") for feature in existing_data if "url" in feature.get("properties", {})}
    new_features = [f for f in new_features if f["properties"]["url"] not in existing_urls]

    # Combinar y guardar
    combined_features = existing_data + new_features
    geojson_data = {
        "type": "FeatureCollection",
        "features": combined_features
    }
    try:
        with open(GEOJSON_OUTPUT, "w") as f:
            json.dump(geojson_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error al guardar el archivo GeoJSON: {e}")

def main():
    start_index = load_progress()
    new_features = []

    with ThreadPoolExecutor() as executor:
        future_to_search = {
            executor.submit(search_wikipedia, term): term for term in SEARCH_TERMS[start_index:]
        }

        for future in as_completed(future_to_search):
            term = future_to_search[future]
            try:
                search_results = future.result()
                processed_results = process_entries(search_results)
                new_features.extend(processed_results)
            except Exception as e:
                print(f"Error procesando los resultados de '{term}': {e}")

            # Guardar el progreso después de cada término
            save_progress(SEARCH_TERMS.index(term) + 1)

    # Guardar datos combinados
    merge_and_save_geojson(new_features)
    print("Proceso finalizado con éxito.")

if __name__ == "__main__":
    main()
