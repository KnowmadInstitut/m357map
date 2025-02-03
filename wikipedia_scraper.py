# -*- coding: utf-8 -*-
"""
Wikipedia Scraper Final Optimizado
- Multi-idioma: en, es, fr, de, pt
- Geocodificación avanzada (Photon + Nominatim)
- Fusión histórica con JSON y GeoJSON.
"""
import requests
import json
import logging
import time
import re
import sqlite3
from typing import List, Dict, Optional, Tuple
import spacy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from datetime import datetime
import geojson
import pandas as pd

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MasonicWikiScraper")

# Palabras clave y configuraciones de idioma
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
LANGUAGES = ["en", "es", "fr", "de", "pt"]

# Configuración del geolocalizador
geolocator = Nominatim(user_agent="geo_scraper_v2")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# Cargar modelo NLP
try:
    nlp = spacy.load("en_core_web_sm")
except Exception as e:
    logger.warning(f"Error cargando spaCy: {e}")
    nlp = None

def search_wikipedia(keyword: str, lang: str, max_results=20) -> List[Dict]:
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": f'"{keyword}"',
        "format": "json",
        "srlimit": max_results
    }
    response = requests.get(url, params=params)
    return response.json().get("query", {}).get("search", [])

def enhanced_geocode(location_text: str) -> Optional[Tuple[float, float]]:
    if not location_text:
        return None
    location = geocode(location_text)
    if location:
        return location.longitude, location.latitude
    return None

def process_article(article: Dict, keyword: str, lang: str) -> Dict:
    title = article["title"]
    page_url = f"https://{lang}.wikipedia.org/wiki/{title.replace(' ', '_')}"
    summary = f"Artículo '{title}' sobre '{keyword}' en '{lang}'"

    location_text = extract_nlp_location(title)
    coords = enhanced_geocode(location_text) if location_text else (None, None)

    return {
        "title": title,
        "summary": summary,
        "latitude": coords[1] if coords else None,
        "longitude": coords[0] if coords else None,
        "keyword": keyword,
        "language": lang,
        "url": page_url,
        "apa_reference": f"{title}. Recuperado el {datetime.now().strftime('%d/%m/%Y')} de {page_url}"
    }

def extract_nlp_location(text: str) -> Optional[str]:
    if nlp:
        doc = nlp(text)
        locations = [ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]
        return locations[0] if locations else None
    return None

def load_previous_geojson(path="masonic_data.geojson") -> geojson.FeatureCollection:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return geojson.FeatureCollection(json.load(f)["features"])
    except (FileNotFoundError, json.JSONDecodeError):
        return geojson.FeatureCollection([])

def merge_geojson(new_features: List[geojson.Feature], old_fc: geojson.FeatureCollection) -> geojson.FeatureCollection:
    merged_features = {feat["properties"]["title"]: feat for feat in old_fc.features}
    for feature in new_features:
        title = feature["properties"]["title"]
        merged_features[title] = feature
    return geojson.FeatureCollection(list(merged_features.values()))

def export_geojson(new_data: List[Dict], geojson_path="masonic_data.geojson"):
    new_features = [
        geojson.Feature(geometry=geojson.Point((d["longitude"], d["latitude"])), properties=d)
        for d in new_data if d["longitude"] and d["latitude"]
    ]
    old_fc = load_previous_geojson(geojson_path)
    merged_fc = merge_geojson(new_features, old_fc)
    with open(geojson_path, "w", encoding="utf-8") as f:
        json.dump(merged_fc, f, indent=2)

def export_json(new_data: List[Dict], json_path="masonic_data.json"):
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            old_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        old_data = []
    combined_data = {entry["title"]: entry for entry in old_data + new_data}
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(list(combined_data.values()), f, indent=2)

def main():
    all_articles = []
    for lang in LANGUAGES:
        for keyword in KEYWORDS:
            search_results = search_wikipedia(keyword, lang)
            for article in search_results:
                processed = process_article(article, keyword, lang)
                all_articles.append(processed)

    export_json(all_articles)
    export_geojson(all_articles)
    df = pd.DataFrame(all_articles)
    df.to_parquet("masonic_data.parquet", index=False)

if __name__ == "__main__":
    main()
