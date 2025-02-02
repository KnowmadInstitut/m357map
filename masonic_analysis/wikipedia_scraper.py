import requests
import json
import logging

# ============== CONFIGURACIÓN DEL LOG =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============== CONFIGURACIÓN DE LA API =================
WIKIPEDIA_ENDPOINT = "https://en.wikipedia.org/w/api.php"
KEYWORDS = [
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
]

def search_wikipedia(keyword: str, limit=5) -> list:
    """
    Busca artículos en Wikipedia relacionados con la palabra clave proporcionada.
    """
    params = {
        "action": "query",
        "list": "search",
        "srsearch": keyword,
        "format": "json",
        "srlimit": limit
    }
    response = requests.get(WIKIPEDIA_ENDPOINT, params=params)
    
    if response.status_code == 200:
        return response.json().get("query", {}).get("search", [])
    else:
        logger.error(f"Error al buscar '{keyword}': {response.status_code}")
        return []

def fetch_article_details(title: str) -> dict:
    """
    Obtiene detalles del artículo incluyendo el extracto y las categorías.
    """
    params = {
        "action": "query",
        "prop": "extracts|categories",
        "exintro": True,
        "explaintext": True,
        "titles": title,
        "format": "json"
    }
    response = requests.get(WIKIPEDIA_ENDPOINT, params=params)
    
    if response.status_code == 200:
        pages = response.json().get("query", {}).get("pages", {})
        for _, page in pages.items():
            return {
                "title": page.get("title"),
                "extract": page.get("extract"),
                "categories": [cat.get("title", "") for cat in page.get("categories", [])]
            }
    else:
        logger.error(f"Error al obtener detalles de '{title}': {response.status_code}")
        return {}

def main():
    all_articles = []
    
    for keyword in KEYWORDS:
        logger.info(f"Buscando artículos para la palabra clave: {keyword}")
        articles = search_wikipedia(keyword)
        
        for article in articles:
            details = fetch_article_details(article["title"])
            all_articles.append({
                "keyword": keyword,
                "title": details.get("title"),
                "summary": details.get("extract"),
                "categories": details.get("categories"),
                "pageid": article.get("pageid")
            })
    
    # Guardar resultados en JSON
    with open("wikipedia_data.json", "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)
    
    logger.info("Extracción completada. Datos guardados en wikipedia_data.json.")

if __name__ == "__main__":
    main()
