import pandas as pd
from geopy.distance import great_circle 
from pathlib import Path
import sys
import re # Import the regular expression module

# --- Configuration des Chemins ---
BASE_DIR = Path(__file__).resolve().parent.parent

# --- Fonction "Outil" pour le calcul Géospatial ---
# (count_poi_nearby function remains exactly the same as before)
def count_poi_nearby(station_row, poi_df, radius_meters):
    try:
        station_coords = (station_row['position_lat'], station_row['position_lon'])
        count = 0
        for _, poi_row in poi_df.iterrows():
            poi_coords = (poi_row['lat'], poi_row['lon'])
            distance = great_circle(station_coords, poi_coords).meters
            if distance <= radius_meters:
                count += 1
        return count
    except Exception as e:
        print(f"Erreur de calcul géospatial : {e}", file=sys.stderr)
        return -1

# --- Fonction Principale de Transformation ---
def transform_data(stations_df, poi_metro_df, poi_bus_df):
    """
    Nettoie les données des vélos, supprime le préfixe numérique du nom,
    supprime la colonne 'number', et enrichit avec les données de transport.
    """
    if stations_df.empty:
        print("⚠️  Aucune donnée de station à transformer.")
        return pd.DataFrame()

    print(f"🔄 Nettoyage et transformation de {len(stations_df)} stations...")
    
    # 1. Nettoyage initial (comme avant)
    try:
        df = stations_df.copy()
        df = df.dropna(subset=['position', 'address'])
        df = df[(df['status'] == "OPEN")]

        df['position_lat'] = df['position'].apply(lambda x: x.get('lat'))
        df['position_lon'] = df['position'].apply(lambda x: x.get('lng')) 
        
        df = df.dropna(subset=['position_lat', 'position_lon'])
        df['last_updated'] = pd.to_datetime('now', utc=True)
        
        # --- MODIFICATION 1: Nettoyer le nom de la station ---
        # Utilise une expression régulière pour supprimer "XXXXX - " au début
        df['name'] = df['name'].str.replace(r'^\d+\s*-\s*', '', regex=True).str.strip()
        # ----------------------------------------------------

        # --- MODIFICATION 2: Sélectionner les colonnes SANS 'number' ---
        cleaned_df = df[[
            # 'number', # <-- Commenté pour le supprimer
            'name', 
            'address', 
            'available_bikes', 
            'available_bike_stands', 
            'status',
            'position_lat', 
            'position_lon', 
            'last_updated'
        ]].copy()
        # -----------------------------------------------------------
        
        print(f"✅ Nettoyage terminé. {len(cleaned_df)} stations valides restantes.")
        
    except Exception as e:
        print(f"❌ ERREUR pendant le nettoyage des vélos : {e}", file=sys.stderr)
        return pd.DataFrame() 

    # 2. Enrichissement (comme avant)
    if not poi_metro_df.empty:
        print("Enrichissement avec les stations de métro (rayon 500m)...")
        cleaned_df['metro_stations_nearby_500m'] = cleaned_df.apply(
            count_poi_nearby, axis=1, poi_df=poi_metro_df, radius_meters=500 
        )
    else:
        print("⚠️ Données de métro non disponibles, enrichissement sauté.")
        cleaned_df['metro_stations_nearby_500m'] = 0

    if not poi_bus_df.empty:
        print("Enrichissement avec les arrêts de bus (rayon 200m)...")
        cleaned_df['bus_stops_nearby_200m'] = cleaned_df.apply(
            count_poi_nearby, axis=1, poi_df=poi_bus_df, radius_meters=200 
        )
    else:
        print("⚠️ Données de bus non disponibles, enrichissement sauté.")
        cleaned_df['bus_stops_nearby_200m'] = 0

    print("✅ Enrichissement géospatial terminé.")
    
    # 3. Sauvegarder (comme avant)
    try:
        enriched_file_path = BASE_DIR / "data" / "stations_cleaned_and_enriched.csv"
        cleaned_df.to_csv(enriched_file_path, index=False)
        print(f"Données enrichies sauvegardées dans {enriched_file_path}")
    except Exception as e:
        print(f"❌ Impossible de sauvegarder le CSV enrichi : {e}", file=sys.stderr)

    return cleaned_df

