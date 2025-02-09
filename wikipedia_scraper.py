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

BATCH_SIZE = 100
PROGRESS_FILE = "progress.txt"
WIKIPEDIA_JSON = "wikipedia_data.json"
GEOJSON_OUTPUT = "wikipedia_data.geojson"

# Términos relevantes
SEARCH_TERMS = [
    # Generalidades de la Masonería (General Masonry)
    "Francmasonería", "Freemasonry", "Freemason", "Masons", "Masonería", "Gran Logia",
    "Masonería Simbólica", "Franc-maçonnerie", "Loge maçonnique", "Ordre maçonnique",
    "Maçonnerie symbolique", "Freimaurerei", "Freimaurer", "Symbolische Maurerei",
    "Maçonaria", "Maçon", "Maçonaria Simbólica", "Massoneria", "Loggia Massonica",
    "Massoneria Simbolica", "自由石匠", "Масонство", "Özgür Mason", "Ahiman Rezon",
    "Francmasonería Universal", "Arte Real", "Royal Art", "Art Royal", "Arte Reale",
    "Königliche Kunst", "Gran Oriente", "Grande Oriente", "Grand Orient",
    "Vereinigte Großloge", "Grande Loja", "Grande Loge", "大共氏会",
    "Universal Brotherhood", "Hermandad Masónica", "Libertad, Igualdad y Fraternidad",
    "Irmandade Maçônica", "Fraternité Maçonnique", "Universal Brotherhood", "国际兄弟情谊",

    # Órdenes y Ritos Masónicos (Masonic Orders and Rites)
    "Rito Escocés Antiguo y Aceptado", "Ancient and Accepted Scottish Rite",
    "Rite Écossais Ancien et Accepté", "Schottischer Ritus", "约克仪式", "York Rite",
    "Rito de York", "Knights Templar", "Caballeros Templarios", "Cavaleiros Templários",
    "Chevaliers du Temple", "Масонский орден тамплиеров", "Rito Escocés Rectificado",
    "Rite Écossais Rectifié", "Rito Escocês Retificado", "Gran Campamento Templario de los Estados Unidos",
    "Memphis-Misraim Rite", "Órdenes de la Cruz Roja",

    # Historia y Documentos Fundacionales (Historical Documents)
    "Constituciones de Anderson", "Anderson's Constitutions", "Constitutions d’Anderson",
    "Estatutos de Schaw", "Schaw Statutes", "Schaw-Statuten", "Manuscrito Regius",
    "Regius Manuscript", "Manuscrit Regius", "Charter of Larmenius", "Ahiman Rezon",
    "Declaración de York", "Wilhelmsbad Convention", "Convención de Wilhelmsbad",
    "St. Clair Charters", "Bula Omne Datum Optimum", "Actas del Gran Oriente",
    "Cartas Patentes Masónicas",

    # Personalidades y Miembros Destacados (Personalities and Notable Members)
    "Hiram Abiff", "Jacques de Molay", "Hugh de Payens", "Bernard de Clairvaux",
    "Baphomet", "Cruz Templaria", "Escudo de los Templarios", "Eliashib", "Salomón",
    "Rey Hiram", "Godofredo de Bouillon", "San Juan Bautista", "Tomás de Aquino",
    "St. Bernard of Clairvaux", "Walter Leslie Wilmshurst", "曼约斯里",
    # Miembros destacados
    "Benjamin Franklin", "George Washington", "Winston Churchill", "Simón Bolívar",
    "Wolfgang Amadeus Mozart", "Giuseppe Garibaldi", "Henry Ford", "Voltaire",
    "Johann Wolfgang von Goethe", "Rudyard Kipling", "Theodore Roosevelt",
    "Edward VII", "Alexander Fleming", "Franklin D. Roosevelt", "Salvador Allende",
    "Lyndon B. Johnson", "Louis Armstrong",

    # Estructuras Masónicas y Lugares (Masonic Structures and Places)
    "Logia de la Antigüedad", "Lodge of Antiquity", "Loge de l’Antiquité",
    "Gran Logia", "Grand Lodge", "Grande Loge", "大共氏会", "Roslin Chapel",
    "Monte del Templo", "St. Thomas of Acon", "Capilla de los Templarios",
    "Catedral Gótica y Masonería", "Gothic Cathedral and Masonry",
    "Cathédrale Gothique et Maçonnerie", "Asilo del Temple", "Templo Escocés",
    "Vereinigte Großloge von Deutschland", "Monte Moriah",

    # Conceptos Simbólicos y Filosóficos (Symbolic and Philosophical Concepts)
    "Arte Real", "Royal Art", "Art Royal", "Arte Reale", "Arte Real", "Königliche Kunst",
    "Egregor", "Égrégore", "Landmarks of Freemasonry", "Landmarks Masónicos",
    "Landmarks der Freimaurerei", "Landmarks Maçonniques", "Landmarks Maçônicos",
    "规格的标记", "Hombre Libre y de Buenas Costumbres", "Fraternidad Universal",
    "Hermetismo Masónico", "Virtudes Cardinales", "Secreto Masónico", "Alquimia Espiritual",

    # Rituales y Prácticas (Rituals and Practices)
    "Libación Quinta", "Oración del Comendador", "Promesa de Fidelidad", "Juramento del Templo",
    "Vigilia de Armas", "Rito de Iniciación", "Salve Templaria", "Tenidas Solemnes",
    "Custodia del Santo Sepulcro", "Conclaves", "Observancia de Pascua", "Procesión del Templo",
    "Observancia de Navidad", "Rito Templario", "Ritual Templario", "Ordo Militum Christi",

    # Grandes Logias Reconocidas (Grand Lodges)
    "Gran Logia Unida de Inglaterra", "Grand Lodge of England", "Grande Loge Unie d'Angleterre",
    "Grande Loja Unida da Inglaterra", "Vereinigte Großloge von England",
    "Gran Logia de Argentina", "Gran Logia de México", "Gran Logia de España",
    "Grand Lodge of Japan", "Gran Logia Alpina de Suiza", "Grande Oriente do Brasil",
    "Gran Logia de Colombia", "Grande Loge Nationale Française", "大不列顿联合大共氏会",

    # Términos relacionados con la Antimasonería (Anti-Masonry)
    "Antimasonería", "Anti-Freemasonry", "Antimaçonaria", "Antimaisonnerie",
    "Антимасонство", "反共氏会", "Masonluk karşıtı"
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
            return [lon, lat]
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
                    "type": "Point" if coordinates else "None",
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

    # Actualizar criterio de eliminación de duplicados
    existing_urls = {feature["properties"]["url"] for feature in existing_data if "url" in feature["properties"]}
    filtered_features = [f for f in new_features if f["properties"]["url"] not in existing_urls]

    # Verificación: Agregar aunque no haya nuevas entradas visibles
    if not filtered_features and existing_data:
        print("No hay nuevas entradas únicas. GeoJSON no modificado.")
    else:
        # Combinar y guardar
        combined_features = existing_data + filtered_features
        geojson_data = {
            "type": "FeatureCollection",
            "features": combined_features
        }
        try:
            with open(GEOJSON_OUTPUT, "w") as f:
                json.dump(geojson_data, f, ensure_ascii=False, indent=2)
            print(f"Guardado exitoso en {GEOJSON_OUTPUT}")
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
                print(f"Error procesando los resultados para '{term}': {e}")

    merge_and_save_geojson(new_features)
    save_progress(start_index + len(new_features))
    print("Proceso completado.")

if __name__ == "__main__":
    main()
