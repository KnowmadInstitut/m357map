# -*- coding: utf-8 -*-
"""
Masonic Wikipedia Research Tool - Ultimate Edition

Características principales:
1. Búsqueda multilingüe con paginación avanzada
2. Sistema de caché inteligente (SQLite + Memoria)
3. Geocodificación híbrida (Nominatim + Photon) con priorización
4. Extracción avanzada de entidades (ubicaciones, fechas, organizaciones)
5. Fusión inteligente de datos históricos
6. Exportación multi-formato (JSON/GeoJSON/Parquet)
7. Sistema de priorización automática
8. Control de tasa de solicitudes (Rate Limiting)
"""

import requests
import json
import logging
import sqlite3
import time
import re
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import spacy
import pandas as pd
import geojson
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuración principal
class Config:
    LANGUAGES = ["en", "es", "fr", "de", "pt"]
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
    "Lessing et la Franc-maçonnerie allemande", "Royal Art", "Arte Real", "Art Royal", "Königliche Kunst"
]
    CACHE_DB = "masonic_data.db"
    REQUEST_TIMEOUT = 20
    RATE_LIMIT = 45  # Llamadas por minuto
    MAX_WORKERS = 8
    NLP_MODEL = "en_core_web_sm"

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(module)s] - %(message)s",
    handlers=[
        logging.FileHandler("masonic_research.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MasonicResearch")

class GeoCache:
    """Sistema de caché geoespacial inteligente"""
    def __init__(self):
        self.conn = sqlite3.connect(Config.CACHE_DB)
        self._init_db()
        
    def _init_db(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                place TEXT PRIMARY KEY,
                lat REAL,
                lon REAL,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
    
    def get_location(self, place: str) -> Optional[Tuple[float, float]]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT lat, lon FROM locations 
            WHERE place = ?
            ORDER BY last_used DESC 
            LIMIT 1
        """, (place,))
        result = cursor.fetchone()
        return result if result else None
    
    def save_location(self, place: str, lat: float, lon: float):
        self.conn.execute("""
            INSERT OR REPLACE INTO locations (place, lat, lon)
            VALUES (?, ?, ?)
        """, (place, lat, lon))
        self.conn.commit()

class ArticleProcessor:
    """Procesador de artículos con capacidades NLP"""
    def __init__(self):
        try:
            self.nlp = spacy.load(Config.NLP_MODEL)
        except Exception as e:
            logger.warning(f"Error cargando modelo NLP: {e}")
            self.nlp = None
    
    def extract_entities(self, text: str) -> dict:
        if not self.nlp:
            return {}
            
        doc = self.nlp(text)
        return {
            "locations": [ent.text for ent in doc.ents if ent.label_ in ("GPE", "LOC")],
            "dates": [ent.text for ent in doc.ents if ent.label_ == "DATE"],
            "organizations": [ent.text for ent in doc.ents if ent.label_ == "ORG"]
        }

class GeoCoder:
    """Servicio de geocodificación con múltiples proveedores"""
    def __init__(self):
        self.geolocator = Nominatim(user_agent="masonic_geo_v3")
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=1)
        self.cache = GeoCache()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
    def get_coordinates(self, place: str) -> Optional[Tuple[float, float]]:
        if not place:
            return None
            
        # Verificar caché primero
        if cached := self.cache.get_location(place):
            return cached
            
        # Intentar con Nominatim
        try:
            location = self.geocode(place)
            if location:
                self.cache.save_location(place, location.latitude, location.longitude)
                return (location.latitude, location.longitude)
        except Exception as e:
            logger.warning(f"Error Nominatim: {str(e)}")
        
        # Fallback a Photon
        try:
            response = requests.get(
                "https://photon.komoot.io/api/",
                params={"q": place},
                timeout=10
            )
            if response.status_code == 200:
                features = response.json().get("features", [])
                if features:
                    coords = features[0]["geometry"]["coordinates"]
                    self.cache.save_location(place, coords[1], coords[0])
                    return (coords[1], coords[0])
        except Exception as e:
            logger.warning(f"Error Photon: {str(e)}")
        
        return None

@sleep_and_retry
@limits(calls=Config.RATE_LIMIT, period=60)
def wiki_search(keyword: str, lang: str) -> List[dict]:
    """Búsqueda avanzada en Wikipedia con paginación"""
    base_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": f'"{keyword}" incategory:"Masonic_buildings"',
        "format": "json",
        "srlimit": 50,
        "srprop": "size|wordcount|timestamp|snippet"
    }
    
    results = []
    while True:
        try:
            response = requests.get(base_url, params=params, timeout=Config.REQUEST_TIMEOUT)
            data = response.json()
            results.extend(data.get("query", {}).get("search", []))
            
            if "continue" not in data:
                break
                
            params["sroffset"] = data["continue"]["sroffset"]
            time.sleep(0.5)  # Respeta el rate limiting
        except Exception as e:
            logger.error(f"Error en búsqueda: {str(e)}")
            break
    
    return results

def process_article(article: dict, lang: str, keyword: str, geocoder: GeoCoder, nlp_processor: ArticleProcessor) -> dict:
    """Procesamiento completo de un artículo"""
    title = article.get("title", "")
    snippet = article.get("snippet", "")
    
    # Extracción de entidades
    entities = nlp_processor.extract_entities(f"{title} {snippet}")
    main_location = entities["locations"][0] if entities["locations"] else None
    
    # Geocodificación
    coords = geocoder.get_coordinates(main_location) if main_location else None
    
    # Priorización
    priority = 1
    if "grand lodge" in title.lower():
        priority = 3
    elif "temple" in title.lower():
        priority = 2
    
    return {
        "title": title,
        "url": f"https://{lang}.wikipedia.org/wiki/{title.replace(' ', '_')}",
        "lang": lang,
        "keyword": keyword,
        "entities": entities,
        "coordinates": coords,
        "priority": priority,
        "wordcount": article.get("wordcount", 0),
        "timestamp": datetime.now().isoformat()
    }

def merge_data(old_data: list, new_data: list) -> list:
    """Fusión inteligente de datos manteniendo históricos"""
    merged = {}
    for item in old_data + new_data:
        key = f"{item['lang']}_{item['title']}"
        if key not in merged or item["priority"] > merged[key]["priority"]:
            merged[key] = item
    return list(merged.values())

def export_data(data: list):
    """Exportación multi-formato con validación"""
    # Exportar JSON
    with open("masonic_data.json", "w") as f:
        json.dump(data, f, indent=2)
    
    # Exportar GeoJSON
    features = [
        geojson.Feature(
            geometry=geojson.Point((item["coordinates"][1], item["coordinates"][0])) if item["coordinates"] else None,
            properties=item
        ) for item in data if item["coordinates"]
    ]
    with open("masonic_data.geojson", "w") as f:
        geojson.dump(geojson.FeatureCollection(features), f, indent=2)
    
    # Exportar Parquet
    df = pd.DataFrame(data)
    df.to_parquet("masonic_data.parquet", index=False)

def main():
    """Flujo principal de ejecución"""
    logger.info("Iniciando investigación masónica en Wikipedia")
    start_time = time.time()
    
    # Inicializar componentes
    geocoder = GeoCoder()
    nlp_processor = ArticleProcessor()
    all_results = []
    
    # Cargar datos históricos
    try:
        with open("masonic_data.json") as f:
            historical_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        historical_data = []
    
    # Procesamiento concurrente
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        futures = []
        for lang in Config.LANGUAGES:
            for keyword in Config.KEYWORDS:
                futures.append(executor.submit(
                    wiki_search, keyword=keyword, lang=lang
                ))
        
        for future in as_completed(futures):
            try:
                search_results = future.result()
                with ThreadPoolExecutor(max_workers=4) as article_executor:
                    processed = list(article_executor.map(
                        lambda art: process_article(art, lang, keyword, geocoder, nlp_processor),
                        search_results
                    ))
                    all_results.extend(processed)
            except Exception as e:
                logger.error(f"Error procesando lote: {str(e)}")
    
    # Fusionar y exportar datos
    final_data = merge_data(historical_data, all_results)
    export_data(final_data)
    
    logger.info(f"""
    Proceso completado exitosamente!
    - Artículos procesados: {len(final_data)}
    - Tiempo total: {time.time() - start_time:.2f} segundos
    - Datos guardados en: 
      • masonic_data.json
      • masonic_data.geojson
      • masonic_data.parquet
    """)

if __name__ == "__main__":
    main()
