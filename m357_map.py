# -*- coding: utf-8 -*-
"""M357_MAP.ipynb"""

import feedparser
import re
import json
import time
import os
from geopy.geocoders import Nominatim

# Configuración
RSS_FEEDS = [
    # Feeds 
    "https://www.google.com/alerts/feeds/08823391955851607514/18357020651463187477",
    "https://www.google.com/alerts/feeds/08823391955851607514/434625937666013668",
    "https://www.google.com/alerts/feeds/08823391955851607514/303056625914324165",
    "https://www.google.com/alerts/feeds/08823391955851607514/9378709536916495456",
    "https://www.google.com/alerts/feeds/08823391955851607514/17243776362555978691",
    "https://www.google.com/alerts/feeds/08823391955851607514/15847044508852608532",
    "https://www.google.com/alerts/feeds/08823391955851607514/6833079353494005014",
    "https://www.google.com/alerts/feeds/08823391955851607514/5572981003473119348",
    "https://www.google.com/alerts/feeds/08823391955851607514/17383807687186980718",
    "https://www.google.com/alerts/feeds/08823391955851607514/11043471059141282309",
    "https://www.google.com/alerts/feeds/08823391955851607514/13877290848809114470",
    "https://www.google.com/alerts/feeds/08823391955851607514/10413993926495102043",
    "https://www.google.com/alerts/feeds/08823391955851607514/2031900511198117844",
    "https://www.google.com/alerts/feeds/08823391955851607514/16568355059505461850",
    "https://www.google.com/alerts/feeds/08823391955851607514/16568355059505461178",
    "https://www.google.com/alerts/feeds/08823391955851607514/7760122889210870690",
    "https://www.google.com/alerts/feeds/08823391955851607514/15183025294765855574",
    "https://www.google.com/alerts/feeds/08823391955851607514/4297759070181606765",
    "https://www.google.com/alerts/feeds/08823391955851607514/11630540178333861502",
    "https://www.google.com/alerts/feeds/08823391955851607514/15251611368669093385",
    "https://www.google.com/alerts/feeds/08823391955851607514/9684782093161547179",
    "https://www.google.com/alerts/feeds/08823391955851607514/8744244600052796540",
    "https://www.google.com/alerts/feeds/08823391955851607514/357094683772830109",
    "https://www.google.com/alerts/feeds/08823391955851607514/13155130439785831467",
    "https://www.google.com/alerts/feeds/08823391955851607514/15809012670835506226",
    "https://www.google.com/alerts/feeds/08823391955851607514/14458568452294133843",
    "https://www.google.com/alerts/feeds/08823391955851607514/3528049070088672707",
    "https://www.google.com/alerts/feeds/08823391955851607514/11937818240173291166"
]

MASTER_JSON = "master_data.json"
OUTPUT_GEOJSON = "masoneria_alertas.geojson"

# Configuración para geocodificación
USE_GEOCODING = True
geolocator = Nominatim(user_agent="masoneria_geolocator")

# Funciones auxiliares (las mismas que ya tienes)
# ...

# Script principal (sin cambios)
if __name__ == "__main__":
    main()
