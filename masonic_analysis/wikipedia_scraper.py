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
    # América del Norte
    "Benjamin Franklin", "George Washington", "Theodore Roosevelt",
    "Thomas Jefferson", "Alexander Fleming", "Harry S. Truman",
    "William Howard Taft", "Andrew Jackson", "James Monroe",
    
    # Canadá
    "John A. Macdonald", "William Lyon Mackenzie King", "Timothy Eaton",
    
    # América Central y el Caribe
    "José Martí", "Juan Pablo Duarte", "Carlos Manuel de Céspedes",
    "Máximo Gómez", "Gregorio Luperón", "Antonio Maceo",

    # América del Sur
    "Simón Bolívar", "Salvador Allende", "José de San Martín",
    "Domingo Faustino Sarmiento", "Andrés Bello", "Bernardino Rivadavia",
    "Arturo Prat", "Rafael Núñez", "Francisco de Paula Santander",
    "José Gervasio Artigas", "Joaquim Nabuco", "José Bonifácio de Andrada e Silva",

    # Europa Occidental
    "Winston Churchill", "Wolfgang Amadeus Mozart", "Voltaire",
    "Giuseppe Garibaldi", "Rudyard Kipling", "Johann Wolfgang von Goethe",
    "Henry Ford", "Edward VII", "Lyndon B. Johnson",
    "Paul-Henri Spaak", "Robert Burns", "Arthur Conan Doyle",
    "Frédéric Bartholdi", "Émile Littré", "Pierre Simon Laplace",

    # Europa del Este
    "Aleksandr Pushkin", "Lajos Kossuth", "Ignacy Jan Paderewski",
    "Tadeusz Kościuszko", "Józef Piłsudski", "Ferenc Deák",
    "Boris Yeltsin", "Sergei Witte", "Mikhail Gorbachev",

    # Europa del Norte
    "Oscar II de Suecia", "Dag Hammarskjöld", "Henrik Ibsen",
    "Christian Michelsen", "Carl Gustaf Emil Mannerheim", "Niels Bohr",
    "Haakon VII", "Fridtjof Nansen", "Emanuel Swedenborg",

    # Europa del Sur
    "Giuseppe Mazzini", "Francesco Crispi", "Fernando Pessoa",
    "António de Oliveira Salazar", "Giovanni Pascoli", "Enrico Fermi",
    "Giosuè Carducci", "Manuel de Arriaga", "Teófilo Braga",

    # África
    "Nelson Mandela", "Cecil Rhodes", "Jan Smuts",
    "Saad Zaghloul", "Ahmed Lutfi el-Sayed", "Agostinho Neto",
    "Mohammed V de Marruecos", "Hassan II", "Houphouët-Boigny",

    # Asia
    "Sun Yat-sen", "Mustafa Kemal Atatürk", "José Rizal",
    "Andrés Bonifacio", "Sukarno", "Rabindranath Tagore",
    "Chiang Kai-shek", "Hassan al-Banna", "Tunku Abdul Rahman",
    "Mohammad Hatta", "Gamal Abdel Nasser (controvertido)", "Prince Tokugawa Iesato",

    # Oceanía
    "Ernest Rutherford", "Edmund Barton", "Charles Kingsford Smith",
    "William Ferguson Massey", "Richard Seddon", "John Forrest", 

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
    "United Grand Lodge of England", "Gran Logia Unida de Inglaterra", "Grande Loge Unie d'Angleterre",
    "Vereinigte Großloge von England", "Grande Loja Unida da Inglaterra",

    "Grand Lodge of Scotland", "Gran Logia de Escocia", "Grande Loge d'Écosse",
    "Großloge von Schottland", "Grande Loja da Escócia",

    "Grand Lodge of Ireland", "Gran Logia de Irlanda", "Grande Loge d'Irlande",
    "Großloge von Irland", "Grande Loja da Irlanda",

    "Grand Lodge of Spain", "Gran Logia de España", "Grande Loge d'Espagne",
    "Großloge von Spanien", "Grande Loja da Espanha",

    "Grande Loge Nationale Française", "Gran Logia Nacional de Francia", "Grand National Lodge of France",
    "Nationale Großloge von Frankreich", "Grande Loja Nacional da França",

    "Grand Lodge Alpina of Switzerland", "Gran Logia Alpina de Suiza", "Grande Loge Alpina de Suisse",
    "Alpine Großloge der Schweiz", "Grande Loja Alpina da Suíça",

    "Grand Lodge of Japan", "Gran Logia de Japón", "Grande Loge du Japon",
    "Großloge von Japan", "Grande Loja do Japão",

    "Grand Lodge of Mexico", "Gran Logia de México", "Grande Loge du Mexique",
    "Großloge von Mexiko", "Grande Loja do México",

    "Grand Lodge of Argentina", "Gran Logia de Argentina", "Grande Loge d'Argentine",
    "Großloge von Argentinien", "Grande Loja da Argentina",

    "Grand Lodge of Canada", "Gran Logia de Canadá", "Grande Loge du Canada",
    "Großloge von Kanada", "Grande Loja do Canadá",

    "Grande Oriente do Brasil", "Gran Oriente de Brasil", "Grand Orient du Brésil",
    "Großorient von Brasilien", "Grande Loja do Brasil",

    "Grand Lodge of Colombia", "Gran Logia de Colombia", "Grande Loge de Colombie",
    "Großloge von Kolumbien", "Grande Loja da Colômbia",

    "Grand Lodge of Italy", "Gran Logia de Italia", "Grande Loge d'Italie",
    "Großloge von Italien", "Grande Loja da Itália",

    "Grand Lodge of the Philippines", "Gran Logia de Filipinas", "Grande Loge des Philippines",
    "Großloge der Philippinen", "Grande Loja das Filipinas",

    "Grand Lodge of Chile", "Gran Logia de Chile", "Grande Loge du Chili",
    "Großloge von Chile", "Grande Loja do Chile",

    "Grand Lodge of Venezuela", "Gran Logia de Venezuela", "Grande Loge du Venezuela",
    "Großloge von Venezuela", "Grande Loja da Venezuela",

    "Grand Lodge of Ecuador", "Gran Logia de Ecuador", "Grande Loge de l'Équateur",
    "Großloge von Ecuador", "Grande Loja do Equador",

    "Grand Lodge of Peru", "Gran Logia de Perú", "Grande Loge du Pérou",
    "Großloge von Peru", "Grande Loja do Peru",

    "Grand Lodge of Bolivia", "Gran Logia de Bolivia", "Grande Loge de Bolivie",
    "Großloge von Bolivien", "Grande Loja da Bolívia",

    "Grand Lodge of Paraguay", "Gran Logia de Paraguay", "Grande Loge du Paraguay",
    "Großloge von Paraguay", "Grande Loja do Paraguai",

    "Grand Lodge of Uruguay", "Gran Logia de Uruguay", "Grande Loge de l'Uruguay",
    "Großloge von Uruguay", "Grande Loja do Uruguai",

    "Grand Lodge of Costa Rica", "Gran Logia de Costa Rica", "Grande Loge du Costa Rica",
    "Großloge von Costa Rica", "Grande Loja da Costa Rica",

    "Grand Lodge of Panama", "Gran Logia de Panamá", "Grande Loge du Panama",
    "Großloge von Panama", "Grande Loja do Panamá",

    "Grand Lodge of Honduras", "Gran Logia de Honduras", "Grande Loge du Honduras",
    "Großloge von Honduras", "Grande Loja das Honduras",

    "Grand Lodge of El Salvador", "Gran Logia de El Salvador", "Grande Loge du Salvador",
    "Großloge von El Salvador", "Grande Loja de El Salvador",

    "Grand Lodge of Guatemala", "Gran Logia de Guatemala", "Grande Loge du Guatemala",
    "Großloge von Guatemala", "Grande Loja da Guatemala",

    "Grand Lodge of Nicaragua", "Gran Logia de Nicaragua", "Grande Loge du Nicaragua",
    "Großloge von Nicaragua", "Grande Loja da Nicarágua"

    # Términos relacionados con la Antimasonería (Anti-Masonry)
    "Antimasonería", "Anti-Freemasonry", "Antimaçonaria", "Antimaisonnerie",
    "Антимасонство", "反共氏会", "Masonluk karşıtı"
    # Añadir más términos según la optimización final...
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
    """Realiza una búsqueda en Wikipedia y devuelve las entradas relevantes."""
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": term,
        "srlimit": 50  # Máximo por consulta
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("query", {}).get("search", [])
    except requests.RequestException as e:
        print(f"Error en la búsqueda de Wikipedia para el término '{term}': {e}")
        return []

def get_article_details(title, lang="en"):
    """Obtiene detalles del artículo dado un título."""
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|coordinates|pageimages",
        "exintro": True,
        "explaintext": True,
        "titles": title,
        "pithumbsize": 500  # Imagen de previsualización
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
    """Convierte las coordenadas de Wikipedia en formato de lat/lon para GeoJSON."""
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
    """Procesa un lote de artículos de Wikipedia."""
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
                    "url": details["url"],
                    "description": details["description"],
                    "image": details["image"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "language": lang
                }
            })
    return results

def merge_and_save_geojson(new_features):
    """Combina los resultados nuevos con los existentes y guarda el GeoJSON."""
    existing_data = []
    if os.path.exists(GEOJSON_OUTPUT):
        try:
            with open(GEOJSON_OUTPUT, "r") as f:
                existing_data = json.load(f).get("features", [])
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error al cargar el archivo GeoJSON existente: {e}")

    # Eliminar duplicados basados en URL y título
    existing_urls_titles = {(feature["properties"]["url"], feature["properties"]["title"]) for feature in existing_data}
    new_features = [f for f in new_features if (f["properties"]["url"], f["properties"]["title"]) not in existing_urls_titles]

    if not new_features:
        print("No hay nuevos datos para guardar.")
        return

    # Combinar y guardar
    combined_features = existing_data + new_features
    geojson_data = {
        "type": "FeatureCollection",
        "features": combined_features
    }
    with open(GEOJSON_OUTPUT, "w") as f:
        json.dump(geojson_data, f, ensure_ascii=False, indent=2)

    # Actualizar JSON secundario para referencias
    with open(WIKIPEDIA_JSON, "w") as f:
        json.dump({"features": combined_features}, f, ensure_ascii=False, indent=2)

    print(f"GeoJSON actualizado: {len(new_features)} nuevas entradas agregadas.")

def main():
    # Cargar el progreso actual
    progress = load_progress()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for term in SEARCH_TERMS[progress:]:
            for lang in ["en", "es", "fr", "de", "pt"]:  # Idiomas a buscar
                futures[executor.submit(search_wikipedia, term, lang)] = (term, lang)

        new_features = []
        for future in as_completed(futures):
            term, lang = futures[future]
            try:
                articles = future.result()
                processed_entries = process_entries(articles, lang)
                new_features.extend(processed_entries)
            except Exception as e:
                print(f"Error procesando el término '{term}' en '{lang}': {e}")

    # Guardar los resultados en GeoJSON y JSON
    merge_and_save_geojson(new_features)
    save_progress(len(SEARCH_TERMS))

if __name__ == "__main__":
    main()
