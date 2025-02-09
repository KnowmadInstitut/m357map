# -*- coding: utf-8 -*-
"""
masonic_wikipedia_scraper.py

Características principales:
1) Búsqueda multilingüe con paginación avanzada (Wikipedia).
2) Caché persistente en SQLite para geocodificación (GeoCache).
3) Geocodificación híbrida (Nominatim + Photon).
4) Extracción de entidades con spaCy (p. ej., lugar, GPE).
5) Fusión inteligente de datos históricos (mantiene JSON anterior).
6) Exportación a JSON, GeoJSON y Parquet.
7) Sistema de priorización (términos como 'Grand Lodge', etc.).
8) Control de tasa de solicitudes (Rate Limiting con ratelimit + RateLimiter).

Requerimientos:
    pip install requests spacy geopy feedparser tenacity ratelimit pandas pyarrow geojson
    python -m spacy download en_core_web_sm   # o el modelo que desees
"""

import json
import logging
import os
import re
import sqlite3
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import spacy
import subprocess
import pandas as pd
import geojson

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from ratelimit import limits, sleep_and_retry

############################################################################
# ===================== CONFIGURACIÓN GLOBAL ===============================
############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("masonic_wiki_scraper.log"), logging.StreamHandler()]
)
logger = logging.getLogger("MasonicWikiScraper")

class Config:
    # 1) Búsqueda en varios idiomas de Wikipedia
    LANGUAGES = ["en", "es", "fr", "de", "pt"]

    # 2) Palabras clave extensas relacionadas con masonería
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

    # 3) Caché en SQLite para la geocodificación
    CACHE_DB = "masonic_data_cache.db"

    # Tiempos y Rate Limit
    REQUEST_TIMEOUT = 15
    RATE_LIMIT_CALLS = 50   # máx 50 llamadas/min
    SPA_MODEL = "en_core_web_sm"

    SRESULTS_PER_PAGE = 50  # Hasta 50 resultados por página
    OUTPUT_JSON = "wikipedia_data.json"
    OUTPUT_GEOJSON = "wikipedia_data.geojson"
    OUTPUT_PARQUET = "wikipedia_data.parquet"


############################################################################
# ============= VERIFICAR DESCARGAR AUTOMÁTICAMENTE MODELO SPACY ==========
############################################################################

try:
    nlp = spacy.load(Config.SPA_MODEL)
except OSError:
    logger.warning(f"Modelo spaCy '{Config.SPA_MODEL}' no encontrado. Descargando e instalando...")
    subprocess.run(["python", "-m", "spacy", "download", Config.SPA_MODEL], check=True)
    nlp = spacy.load(Config.SPA_MODEL)

############################################################################
# ============= RATE LIMITING (con ratelimit) ==============================
############################################################################

@sleep_and_retry
@limits(calls=Config.RATE_LIMIT_CALLS, period=60)
def safe_requests_get(url, params=None):
    """
    Llamada a requests.get con un límite de 50 llamadas por minuto
    """
    return requests.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)

############################################################################
# =============== CACHÉ PERSISTENTE PARA GEOCODIFICACIÓN ===================
############################################################################

class GeoCache:
    """Almacena en SQLite (table: locations) las coords lat/lon de cada lugar."""
    def __init__(self):
        self.conn = sqlite3.connect(Config.CACHE_DB)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                place TEXT PRIMARY KEY,
                lat REAL,
                lon REAL
            )
        """)
        self.conn.commit()

    def get(self, place: str) -> Optional[Tuple[float, float]]:
        c = self.conn.cursor()
        c.execute("SELECT lat, lon FROM locations WHERE place=?", (place,))
        row = c.fetchone()
        return (row[0], row[1]) if row else None

    def set(self, place: str, lat: float, lon: float):
        self.conn.execute("""
            INSERT OR REPLACE INTO locations (place, lat, lon)
            VALUES (?, ?, ?)
        """, (place, lat, lon))
        self.conn.commit()

geo_cache = GeoCache()

############################################################################
# ================ GEOCODIFICACIÓN HÍBRIDA (NOMINATIM + PHOTON) ============
############################################################################

geolocator = Nominatim(user_agent="masonic_scraper_v3")
# RateLimiter para Nominatim
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def geocode_hybrid(location_text: str) -> Tuple[Optional[float], Optional[float]]:
    """
    3) Geocodifica con:
      - Caché local
      - Nominatim con RateLimiter
      - Photon como fallback
    Retorna (lat, lon) o (None, None).
    """
    if not location_text:
        return (None, None)
    
    # Revisar cache
    cached = geo_cache.get(location_text)
    if cached:
        return cached
    
    # Nominatim
    try:
        loc = geocode(location_text)
        if loc:
            lat, lon = loc.latitude, loc.longitude
            geo_cache.set(location_text, lat, lon)
            return (lat, lon)
    except Exception as e:
        logger.warning(f"Nominatim error con '{location_text}': {e}")
    
    # Photon fallback
    try:
        photon_url = f"https://photon.komoot.io/api/?q={location_text}"
        r = safe_requests_get(photon_url)
        if r.status_code == 200:
            data = r.json()
            feats = data.get("features", [])
            if feats:
                best = feats[0]
                lon, lat = best["geometry"]["coordinates"]
                geo_cache.set(location_text, lat, lon)
                return (lat, lon)
    except Exception as e:
        logger.warning(f"Photon error con '{location_text}': {e}")

    return (None, None)

############################################################################
# ================ PAGINACIÓN AVANZADA EN WIKIPEDIA ========================
############################################################################

def advanced_wiki_search(keyword: str, lang: str) -> List[Dict]:
    """
    1) Búsqueda con paginación en Wikipedia:
       Toma 50 resultados x página hasta agotar 'sroffset'.
    """
    base_url = f"https://{lang}.wikipedia.org/w/api.php"
    all_articles = []
    sroffset = 0
    
    while True:
        params = {
            "action": "query",
            "list": "search",
            "srsearch": f'"{keyword}"',
            "format": "json",
            "srlimit": Config.SRESULTS_PER_PAGE,
            "srprop": "size|wordcount|timestamp",
            "sroffset": sroffset
        }
        try:
            resp = safe_requests_get(base_url, params=params)
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("query", {}).get("search", [])
            if not batch:
                break
            all_articles.extend(batch)

            cont = data.get("continue")
            if cont and "sroffset" in cont:
                sroffset = cont["sroffset"]
            else:
                break
        except Exception as e:
            logger.warning(f"Error paginando '{keyword}' en {lang}: {e}")
            break
    
    return all_articles

############################################################################
# ================ EXTRACCIÓN DE ENTIDADES (spaCy) =========================
############################################################################

def extract_location_spacy(text: str) -> Optional[str]:
    """
    4) Usa spaCy para extraer el primer GPE que encuentre en el texto.
    """
    if not text:
        return None
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ == "GPE":
            return ent.text
    return None

############################################################################
# ============= SISTEMA DE PRIORIDAD (EJ. 'GRAND LODGE' => MÁS) ============
############################################################################

def compute_priority(title: str) -> int:
    """
    7) Aumenta prioridad a términos importantes.
    """
    tlower = title.lower()
    if "grand lodge" in tlower:
        return 5
    elif "masonic temple" in tlower:
        return 4
    elif "lodge" in tlower:
        return 3
    else:
        return 1

############################################################################
# ============= PROCESAR UN ARTÍCULO (CREAR OBJETO FINAL) ==================
############################################################################

def process_article(article: Dict, lang: str, keyword: str) -> Optional[Dict]:
    """
    Maneja desempaquetado seguro (lat, lon) or (None, None).
    """
    try:
        page_title = article.get("title", "No Title")
        page_id = article.get("pageid", -1)

        # Extraer localización con spaCy
        loc_text = extract_location_spacy(page_title)
        lat, lon = (None, None)
        if loc_text:
            lat, lon = geocode_hybrid(loc_text)  # Evita error => None => (None, None)

        priority = compute_priority(page_title)

        return {
            "pageid": page_id,
            "keyword": keyword,
            "lang": lang,
            "title": page_title,
            "latitude": lat,
            "longitude": lon,
            "priority": priority,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error en process_article '{article}': {e}")
        return None

############################################################################
# ============ FUSIÓN DE DATOS HISTÓRICOS (JSON) ===========================
############################################################################

def load_previous_data(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def merge_data(old_data: List[Dict], new_data: List[Dict]) -> List[Dict]:
    """
    5) Fusión inteligente: si 'pageid' coincide, actualiza si la priority es mayor.
    Usa (lang + pageid) como identificador.
    """
    merged = {}
    for item in old_data:
        key = f"{item.get('lang','')}_{item.get('pageid','')}"
        merged[key] = item

    for item in new_data:
        key = f"{item.get('lang','')}_{item.get('pageid','')}"
        old_item = merged.get(key)
        if (not old_item) or (item.get("priority",0) > old_item.get("priority",0)):
            merged[key] = item
    
    return list(merged.values())

############################################################################
# =============== EXPORTAR A JSON, GEOJSON, PARQUET ========================
############################################################################

def export_data(final_data: List[Dict]):
    # 1) JSON
    with open(Config.OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    # 2) GeoJSON
    feats = []
    for obj in final_data:
        lat = obj.get("latitude")
        lon = obj.get("longitude")
        if lat is not None and lon is not None:
            props = dict(obj)
            # si no quieres lat/lon duplicados, podrías removerlos de props
            geometry = geojson.Point((lon, lat))
            feat = geojson.Feature(geometry=geometry, properties=props)
            feats.append(feat)
    fc = geojson.FeatureCollection(feats)
    with open(Config.OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
        geojson.dump(fc, f, ensure_ascii=False, indent=2)
    
    # 3) Parquet
    try:
        df = pd.DataFrame(final_data)
        df.to_parquet(Config.OUTPUT_PARQUET, index=False)
    except ImportError:
        logger.warning("pyarrow/parquet no instalado. Saltando export a Parquet.")
    
    logger.info(f"Export final: JSON='{Config.OUTPUT_JSON}', GEOJSON='{Config.OUTPUT_GEOJSON}', PARQUET='{Config.OUTPUT_PARQUET}'")

############################################################################
# ========================= FUNCIÓN PRINCIPAL ==============================
############################################################################

def main():
    # Cargar histórico
    old_data = load_previous_data(Config.OUTPUT_JSON)

    # Realizar búsquedas concurrentes
    results = []
    with ThreadPoolExecutor() as executor:
        future_map = {}
        for kw in Config.KEYWORDS:
            for lang in Config.LANGUAGES:
                fut = executor.submit(advanced_wiki_search, kw, lang)
                future_map[fut] = (kw, lang)
        
        for fut in as_completed(future_map):
            keyword, lang = future_map[fut]
            try:
                articles = fut.result()
                for art in articles:
                    processed = process_article(art, lang, keyword)
                    if processed:
                        results.append(processed)
            except Exception as e:
                logger.error(f"Error en {keyword} ({lang}): {e}")

    # Fusionar con datos viejos
    final_merged = merge_data(old_data, results)

    # Exportar
    export_data(final_merged)

    logger.info(f"Proceso finalizado. Total de artículos: {len(final_merged)}")

if __name__ == "__main__":
    main()
