import pandas as pd
from pathlib import Path
import sys

# --- Configuration des Chemins ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "tisseo_gtfs_v2" 

# --- Fonctions Logiques (Le "Moteur" GTFS) ---

def load_gtfs_data(data_dir):
    """
    Charge les 4 fichiers GTFS essentiels (stops, routes, trips, stop_times) 
    depuis le dossier /data/tisseo_gtfs_v2.
    """
    try:
        stops_df = pd.read_csv(data_dir / "stops.txt", low_memory=False)
        routes_df = pd.read_csv(data_dir / "routes.txt", low_memory=False)
        trips_df = pd.read_csv(data_dir / "trips.txt", low_memory=False)
        stop_times_df = pd.read_csv(data_dir / "stop_times.txt", low_memory=False)
        
        return stops_df, routes_df, trips_df, stop_times_df
        
    except FileNotFoundError as e:
        print(f"❌ ERREUR: Fichier GTFS manquant : {e}", file=sys.stderr)
        print(f"💡 Avez-vous placé tous les fichiers .txt dans le dossier '{DATA_DIR}' ?", file=sys.stderr)
        return None
    except Exception as e:
        print(f"❌ ERREUR inattendue lors du chargement des fichiers GTFS : {e}", file=sys.stderr)
        return None

def get_stops_by_type():
    """
    Tâche ETL : Joint les 4 tables GTFS, identifie le type (Métro/Bus),
    puis **regroupe par nom de station** pour obtenir des stations uniques.
    """
    print("🏛️  Chargement et jointure des fichiers GTFS...")
    
    data = load_gtfs_data(DATA_DIR) 
    if data is None:
        return pd.DataFrame(), pd.DataFrame()

    stops_df, routes_df, trips_df, stop_times_df = data

    # --- Jointures (comme avant) ---
    routes_trips = pd.merge(routes_df, trips_df, on='route_id')
    stop_times_unique = stop_times_df[['trip_id', 'stop_id']].drop_duplicates()
    routes_stops = pd.merge(routes_trips, stop_times_unique, on='trip_id')
    full_data = pd.merge(routes_stops, stops_df, on='stop_id')

    # --- Nettoyage initial (comme avant) ---
    intermediate_stops = full_data[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'route_type']].drop_duplicates()

    # --- NOUVELLE ÉTAPE : Regroupement par Nom ---
    print("...Regroupement des arrêts physiques par nom de station...")
    # On groupe par 'stop_name' et 'route_type'. Pour chaque groupe, 
    # on prend les coordonnées du *premier* arrêt physique trouvé.
    # reset_index() transforme le résultat du groupe en DataFrame.
    unique_stations = intermediate_stops.groupby(['stop_name', 'route_type']).first().reset_index()
    # Note : 'first()' prend la première occurence de lat/lon pour un nom donné.
    # On pourrait aussi faire la moyenne (.mean()), mais 'first' est plus simple.

    print(f"✅ Trouvé {len(unique_stations)} stations nommées uniques (tous types).")

    # --- Séparation Métro / Bus (comme avant, mais sur les données groupées) ---
    
    # route_type 1 = Métro
    metro_stations = unique_stations[unique_stations['route_type'] == 1].copy()
    metro_stations.rename(columns={'stop_name': 'Nom', 'stop_lat': 'lat', 'stop_lon': 'lon'}, inplace=True)
    # Garder seulement les colonnes nécessaires
    metro_stations = metro_stations[['Nom', 'lat', 'lon']] 
    
    # route_type 3 = Bus
    bus_stations = unique_stations[unique_stations['route_type'] == 3].copy()
    bus_stations.rename(columns={'stop_name': 'nom', 'stop_lat': 'lat', 'stop_lon': 'lon'}, inplace=True)
    # Garder seulement les colonnes nécessaires
    bus_stations = bus_stations[['nom', 'lat', 'lon']]
    
    return metro_stations, bus_stations

# --- Fonctions Publiques (inchangées) ---

def extract_metro_stations():
    """
    Extrait les stations de Métro uniques à partir des fichiers GTFS.
    """
    print("Extracting Unique Metro Stations...")
    metro_df, _ = get_stops_by_type()
    if not metro_df.empty:
        print(f"✅ Extrait {len(metro_df)} stations de métro uniques.")
    return metro_df

def extract_bus_stops():
    """
    Extrait les arrêts de Bus uniques à partir des fichiers GTFS.
    """
    print("Extracting Unique Bus Stops...")
    _, bus_df = get_stops_by_type()
    if not bus_df.empty:
        print(f"✅ Extrait {len(bus_df)} arrêts de bus uniques.")
    return bus_df


# --- Bloc de Test (inchangé) ---
if __name__ == "__main__":
    """
    Teste l'extraction GTFS quand on exécute ce fichier directement :
    python src/extract_tisseo.py
    """
    print("🧪 Test de l'extraction des POI (Stations Uniques GTFS)...\n")
    
    metro_df, bus_df = get_stops_by_type()
    
    print("\n--- Stations de Métro Uniques (5 premières) ---")
    print( metro_df.head() )
    
    print("\n--- Arrêts de Bus Uniques (5 premières) ---")
    print( bus_df.head() )
    
    print(f"\nTotal stations de métro trouvées : {len(metro_df)}")
    print(f"Total arrêts de bus trouvés : {len(bus_df)}")
    print("\n✅ Test de 'extract_tisseo.py' terminé.")