import json
from datetime import datetime
from textblob import TextBlob
from langdetect import detect, DetectorFactory
from urllib.parse import urlparse
from geopy.geocoders import Nominatim
import tldextract
from transformers import pipeline  # Importar el modelo de Hugging Face
import os

def main(input_file="new_data.geojson", output_file="references_apa7.txt", analysis_file="analysis_summary.txt"):
    if not os.path.isfile(input_file):
        print(f"Error: No se encontró el archivo {input_file}. Verifica que el proceso de generación haya sido exitoso.")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

DetectorFactory.seed = 0

# Configurar geolocalización
geolocator = Nominatim(user_agent="masonic_analysis_geolocator")

# Inicializar pipeline de resúmenes (BART preentrenado)
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# ========== Diccionario de categorías optimizadas ==========
CATEGORIES = {
    "antimasonico": ["antimasonería", "conspiración masónica", "teoría conspirativa", "nuevo orden mundial", 
                     "ataques contra masones", "destrucción de logias", "control mundial", "propaganda antimasonica"],
    
    "desinformación": ["fake news", "noticias falsas", "información errónea", "desmentido", "bulo masónico", 
                       "manipulación de información", "censura", "distorsión masónica", "manipulación mediática"],
    
    "pro masonico": ["gran logia", "evento masónico", "logia masónica", "hermandad masónica", "educación masónica", 
                     "beneficencia masónica", "avances masónicos", "proyectos de logias", "contribución social"],
    
    "política y masonería": ["relación masonería-gobierno", "influencia masónica en política", "legislación", "tratado", 
                             "parlamento", "colaboración internacional", "gobierno y masonería"],
    
    "historia masónica": ["fundación de logias", "orígenes de la masonería", "figuras históricas de la masonería", 
                          "eventos masónicos históricos", "ritos históricos", "símbolos masónicos", "templos históricos"],
    
    "actualidad masónica": ["noticias masónicas", "última hora masonería", "conferencia masónica", "anuncio masónico", 
                            "eventos masónicos recientes", "crisis masónica", "conflictos masónicos", "comunicado oficial"],
    
    "cultura y entretenimiento": ["cine masónico", "series sobre masonería", "documentales masónicos", "arte masónico", 
                                  "masonería en cultura pop", "representaciones masónicas", "literatura masónica"],
    
    "anuncios y comunicaciones": ["comunicado oficial masónico", "anuncio de logia", "circulares masónicas", "boletines masónicos"],
    
    "obituarios y homenajes": ["obituario masónico", "homenaje a masones", "in memoriam", "funeral masónico"],
    
    "artículos de opinión": ["artículo de opinión masónica", "editorial sobre masonería", "columna de opinión masónica", 
                             "análisis masónico", "ensayo masónico"],
    
    "artículos académicos y estudios": ["artículo académico sobre masonería", "investigación masónica", 
                                        "estudios históricos de la masonería", "tesis sobre masonería", "revistas académicas masónicas"]
}

# ========== Función para geolocalización avanzada ==========
def get_location_details(coords):
    try:
        location = geolocator.reverse(coords, exactly_one=True, timeout=10)
        if location and location.raw.get("address"):
            address = location.raw["address"]
            return {
                "municipio": address.get("city", "Desconocido"),
                "subregion": address.get("state", "Desconocido"),
                "region": address.get("country", "Desconocido"),
                "continente": address.get("continent", "Desconocido"),
                "pais": address.get("country", "Desconocido")
            }
    except Exception as e:
        print(f"Error al obtener ubicación: {str(e)}")
    return {
        "municipio": "Desconocido",
        "subregion": "Desconocido",
        "region": "Desconocido",
        "continente": "Desconocido",
        "pais": "Desconocido"
    }

# ========== Función para identificar fuente del artículo ==========
def get_source_from_url(url):
    parsed_url = urlparse(url)
    domain = tldextract.extract(url).domain
    return domain.capitalize() if domain else "Fuente desconocida"

# ========== Función para obtener idioma ==========
def detect_language(text):
    try:
        return detect(text)
    except Exception:
        return "Desconocido"

# ========== Generar referencias en formato APA ==========
def generate_apa_reference(entry):
    title = entry.get("title", "Sin título").strip()
    link = entry.get("link", "")
    published_date = entry.get("published", "").strip()

    if published_date:
        try:
            date_obj = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%SZ")
            formatted_date = date_obj.strftime("%Y, %B %d")
        except ValueError:
            formatted_date = "n.d."
    else:
        formatted_date = "n.d."

    return f"{title}. ({formatted_date}). Retrieved from {link}\n"

# ========== Análisis de sentimiento ==========
def analyze_sentiment(text):
    blob = TextBlob(text)
    sentiment_score = blob.sentiment.polarity
    if sentiment_score > 0.1:
        return "positivo"
    elif sentiment_score < -0.1:
        return "negativo"
    else:
        return "neutral"

# ========== Clasificación basada en categorías ==========
def categorize_text(text):
    found_categories = []
    for main_category, keywords in CATEGORIES.items():
        if any(keyword.lower() in text.lower() for keyword in keywords):
            found_categories.append(main_category)
    return ", ".join(found_categories) if found_categories else "sin categoría"

# ========== Generar resumen largo utilizando BART ==========
def generate_long_summary(text):
    try:
        summarized_text = summarizer(text, max_length=250, min_length=100, do_sample=False)[0]["summary_text"]
        return summarized_text
    except Exception as e:
        print(f"Error al generar resumen: {str(e)}")
        return text[:500]  # En caso de error, recortar el texto original

# ========== Función principal ==========
def main(input_file="new_data.geojson", output_file="references_apa7.txt", analysis_file="analysis_summary.txt"):
    with open(input_file, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    references = []
    analysis_results = []

    for feature in geojson_data.get("features", []):
        properties = feature.get("properties", {})
        title = properties.get("title", "Sin título").strip()
        description = properties.get("description", "")
        link = properties.get("link", "")
        coords = feature.get("geometry", {}).get("coordinates", [None, None])
        coords_str = f"{coords[1]}, {coords[0]}" if all(coords) else "Desconocido"

        # Generar referencia APA
        reference = generate_apa_reference(properties)
        references.append(reference)

        # Resumen extenso usando BART
        long_summary = generate_long_summary(description)

        # Realizar análisis de sentimiento
        sentiment = analyze_sentiment(description)

        # Clasificar el texto
        category = categorize_text(description)

        # Detectar el idioma
        language = detect_language(description)

        # Detectar fuente del artículo
        source = get_source_from_url(link)

        # Obtener detalles de la ubicación
        location_details = get_location_details(tuple(coords)) if coords[0] and coords[1] else {
            "municipio": "Desconocido",
            "subregion": "Desconocido",
            "region": "Desconocido",
            "continente": "Desconocido",
            "pais": "Desconocido"
        }

        # Resumen del análisis
        analysis_results.append(
            f"Título: {title}\n"
            f"Resumen: {long_summary}\n"
            f"Fuente: {source}\n"
            f"URL: {link}\n"
            f"Sentimiento: {sentiment}\n"
            f"Categoría: {category}\n"
            f"Idioma: {language}\n"
            f"Ubicación: {location_details['municipio']}, {location_details['subregion']}, "
            f"{location_details['region']}, {location_details['continente']}, {location_details['pais']}\n"
            f"Coordenadas: {coords_str}\n\n"
        )

    # Guardar referencias APA
    with open(output_file, "w", encoding="utf-8") as f:
        f.writelines(references)

    # Guardar el resumen del análisis
    with open(analysis_file, "w", encoding="utf-8") as f:
        f.writelines(analysis_results)

if __name__ == "__main__":
    main()
