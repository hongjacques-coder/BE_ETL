#!/usr/bin/env python3
"""
Pipeline ETL "Baby Google Maps" (Projet BE)

Ce script exécute le pipeline ETL complet :
1. Extrait les données "live" des stations de vélo (API JCDecaux).
2. Extrait les données "statiques" des arrêts de métro et de bus (fichiers GTFS).
3. Transforme les données des vélos en les nettoyant.
4. Enrichit les données des vélos avec une analyse géospatiale (proximité des bus/métros).
5. Charge le DataFrame final et enrichi dans la base de données PostgreSQL.
6. Vérifie les données en exécutant des requêtes d'analyse.

Exécuter avec : python main.py
"""

# --- Importations ---
# 1. Extraction
from src.extract_data import extract_stations
from src.extract_tisseo import extract_metro_stations, extract_bus_stops

# 2. Transformation
from src.transform_data import transform_data # <-- Remplacer clean_stations par transform_data

# 3. Chargement et Vérification
from src.load_data import load_to_database, verify_data

def main():
    """Exécute le pipeline ETL complet"""
    print("🚲 Démarrage du pipeline ETL 'Baby Google Maps'...")
    print("=" * 50)
    
    # --- ÉTAPE 1: EXTRACTION ---
    print("\n=== EXTRACTION ===")
    print("📥 Extraction des données sources...")
    
    # Appeler les 3 fonctions d'extraction
    stations_df = extract_stations()
    poi_metro_df = extract_metro_stations()
    poi_bus_df = extract_bus_stops()
    
    # --- ÉTAPE 2: TRANSFORMATION ---
    print("\n=== TRANSFORMATION ===")
    print("🔄 Nettoyage, transformation et enrichissement des données...")
    
    # Appeler la nouvelle fonction de transformation
    # Elle prend 3 DataFrames en entrée
    cleaned_enriched_df = transform_data(stations_df, poi_metro_df, poi_bus_df)
    

    print("\n--- Aperçu des 100 premières lignes enrichies ---")
    print(cleaned_enriched_df.head(10))
   


    # --- ÉTAPE 3: CHARGEMENT (LOAD) ---
    print("\n=== CHARGEMENT (LOAD) ===")
    print("💾 Chargement des données enrichies vers la base de données...")
    
    
    load_to_database(cleaned_enriched_df)
    
    # --- ÉTAPE 4: VÉRIFICATION ---
    print("\n=== VÉRIFICATION ===")
    print("✅ Vérification des données et exécution des analyses...")
    
    
    verify_data()
    
    print("\n🎉 Pipeline ETL terminé avec succès !")
    print("=" * 50)

if __name__ == "__main__":
    main()