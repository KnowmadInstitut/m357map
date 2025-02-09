import json
import gspread
import os
import requests
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from gspread import Cell
import threading

# Configuración de scopes de Google
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Configuración inicial
thread_local = threading.local()

def get_session():
    """Crea una sesión HTTP con reintentos por hilo."""
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        thread_local.session = session
    return thread_local.session

def scrape_and_process(url):
    """Procesa una URL y extrae datos con manejo de errores mejorado."""
    session = get_session()
    try:
        response = session.get(url.strip(), timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Extracción de datos
        title = soup.title.get_text(strip=True) if soup.title else "Sin título"
        summary = soup.find('p').get_text(strip=True) if soup.find('p') else ""
        
        # Detección de idioma con manejo de errores
        try:
            language = detect(summary) if summary else "Desconocido"
        except LangDetectException:
            language = "Desconocido"

        parsed_url = urlparse(url)
        publisher = parsed_url.netloc if parsed_url.netloc else "Desconocido"
        
        # Categorización mejorada
        path = parsed_url.path.lower()
        category = (
            "Artículo" if "article" in path else
            "Evento" if "event" in path else
            "Noticia"
        )
        
        # Lógica de contenido y palabras clave
        content_type = "Educativo" if "historia" in summary.lower() else "General"
        keywords = [word for word in ["masonería", "logia", "historia"] if word in summary.lower()]
        
        return (
            title,
            summary,
            publisher,
            category,
            "General",  # Subcategoría
            content_type,
            ", ".join(keywords),
            "Neutral",  # Emoción (placeholder)
            "Masonería, Historia" if "historia" in summary.lower() else "General",
            language
        )
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Acceso denegado para {url}. Saltando.")
        elif e.response.status_code == 429:
            print(f"Demasiadas solicitudes a {url}. Retentando en el siguiente lote.")
        else:
            print(f"Error HTTP en {url}: {str(e)}")
        return ("Error",) * 10
    
    except Exception as e:
        print(f"Error procesando {url}: {str(e)}")
        return ("Error",) * 10

def main():
    # Configuración de Google Sheets
    creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=scopes)
    gc = gspread.authorize(creds)
    
    # Cargar datos
    sheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1dBQlNHrD6Ww69CcW9x47VIMd4FAay_7-paIlYEy0H8U/edit#gid=0")
    worksheet = sheet.sheet1
    urls = [url for url in worksheet.col_values(10)[1:] if url.strip()]
    
    # Procesamiento paralelo
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(scrape_and_process, urls))
    
    # Preparar actualización masiva
    cell_map = {
        1: 0,   # A: Título
        2: 5,   # B: Tipo contenido
        3: 9,   # C: Idioma
        4: None,# D: Neutral (valor fijo)
        11: 1,  # K: Resumen
        12: 3,  # L: Categoría
        13: 4,  # M: Subcategoría
        15: 2,  # O: Publicador
        17: 6,  # Q: Palabras clave
        18: 7,  # R: Emoción
        19: 8   # S: Temas
    }
    
    cells = []
    for idx, result in enumerate(results):
        if result[0] == "Error":
            continue
        
        row = idx + 2
        for col, res_idx in cell_map.items():
            value = "Neutral" if col == 4 else result[res_idx]
            cells.append(Cell(row=row, col=col, value=value))
    
    # Actualización masiva con manejo de lotes
    BATCH_SIZE = 500
    for i in range(0, len(cells), BATCH_SIZE):
        worksheet.update_cells(cells[i:i+BATCH_SIZE])
    
    print(f"Actualización completada. {len(cells)} celdas modificadas.")

if __name__ == "__main__":
    main()
