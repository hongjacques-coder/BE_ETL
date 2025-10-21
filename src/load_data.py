"""
Module de Chargement des Données

Ce module gère le chargement des données finales et enrichies
dans la base de données PostgreSQL. Il contient aussi des fonctions
de vérification pour s'assurer que tout a bien fonctionné.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys # Pour afficher les erreurs

# --- CONFIGURATION REQUISE ---
# Comme indiqué dans le README, modifiez ces valeurs
# pour correspondre à votre configuration PostgreSQL.
DATABASE_CONFIG = {
    'username': 'jacques',      # <-- REMPLACEZ PAR VOTRE NOM D'UTILISATEUR
    'password': 'Noixcoco1704', # <-- REMPLACEZ PAR VOTRE MOT DE PASSE
    'host': 'localhost',
    'port': '5432',
    'database': 'stations_db'
}
# ------------------------------

def get_connection_string():
    """Construit l'URL de connexion à PostgreSQL."""
    return (
        f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}"
        f"@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"
    )

def load_to_database(df):
    """
    Charge le DataFrame final et enrichi dans la table 'stations' de PostgreSQL.
    """
    if df.empty or not isinstance(df, pd.DataFrame):
        print("⚠️ DataFrame vide ou invalide. Aucune donnée à charger.")
        return

    print(f"💾 Chargement de {len(df)} lignes vers la base de données...")
    
    try:
        connection_string = get_connection_string()
        engine = create_engine(connection_string)
        
        # if_exists='replace' : supprime la table si elle existe et la recrée.
        # C'est parfait pour un pipeline ETL qui tourne régulièrement.
        df.to_sql(
            'stations', 
            engine, 
            if_exists='replace', 
            index=False
        )
        print("✅ Chargement terminé avec succès !")
        
    except Exception as e:
        print(f"❌ ERREUR pendant le chargement des données : {e}", file=sys.stderr)
        print("💡 Assurez-vous que PostgreSQL est démarré et que vos identifiants "
              "dans DATABASE_CONFIG sont corrects.", file=sys.stderr)

def verify_data():
    """
    Exécute des requêtes SQL pour vérifier que les données sont
    bien dans la base et pour montrer des analyses intéressantes.
    """
    print("\n🔍 Vérification des données dans la base...")
    
    try:
        connection_string = get_connection_string()
        engine = create_engine(connection_string)

        # Requête 1: Compter le nombre total de stations chargées
        count_query = "SELECT COUNT(*) as total_stations FROM stations;"
        total_stations = pd.read_sql(count_query, engine).iloc[0]['total_stations']
        print(f"📊 Nombre total de stations dans la base : {total_stations}")
        
        # Requête 2: Montrer les 5 stations les mieux connectées aux bus
        bus_query = """
        SELECT name, bus_stops_nearby_200m
        FROM stations
        ORDER BY bus_stops_nearby_200m DESC
        LIMIT 5;
        """
        top_bus_stations = pd.read_sql(bus_query, engine)
        print("\n🚍 Top 5 des stations les mieux desservies par les bus (à 200m) :")
        print(top_bus_stations.to_string(index=False))
        
        # Requête 3: Montrer les 5 stations les mieux connectées aux métros
        metro_query = """
        SELECT name, metro_stations_nearby_500m
        FROM stations
        ORDER BY metro_stations_nearby_500m DESC
        LIMIT 5;
        """
        top_metro_stations = pd.read_sql(metro_query, engine)
        print("\n🚇 Top 5 des stations les mieux desservies par le métro (à 500m) :")
        print(top_metro_stations.to_string(index=False))

    except Exception as e:
        print(f"❌ ERREUR pendant la vérification des données : {e}", file=sys.stderr)
        print("💡 La table 'stations' existe-t-elle ? Avez-vous exécuté 'database_setup.sql' ?", file=sys.stderr)

def test_database_connection():
    """
    Teste la connexion à la base de données sans charger de données.
    """
    print("🔌 Test de la connexion à la base de données...")
    
    try:
        connection_string = get_connection_string()
        engine = create_engine(connection_string)
        
        # Essayer une requête simple
        result = pd.read_sql("SELECT 1 as test", engine)
        
        if result.iloc[0]['test'] == 1:
            print("✅ Connexion à la base de données réussie !")
            
            # Vérifier si notre table 'stations' existe
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'stations';
            """
            tables = pd.read_sql(tables_query, engine)
            
            if len(tables) == 1:
                print("✅ La table 'stations' existe.")
            else:
                print(f"⚠️  Table 'stations' non trouvée.")
                print("💡 Exécutez 'database_setup.sql' pour créer la table.")
            
            return True
        else:
            print("❌ Test de connexion à la base de données échoué (la requête n'a pas retourné 1).", file=sys.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Échec de la connexion à la base de données : {e}", file=sys.stderr)
        print("💡 Vérifiez vos paramètres dans DATABASE_CONFIG.", file=sys.stderr)
        return False

# --- Bloc de Test ---
if __name__ == "__main__":
    """
    Teste le module de chargement quand on exécute ce fichier directement :
    python src/load_data.py
    """
    print("🧪 Test du module de chargement (Load)...\n")
    
    # 1. Tester la connexion
    if test_database_connection():
        print("\nConnexion OK. Test de chargement de données factices...")
        
        # 2. Créer un petit DataFrame factice pour le test
        dummy_df = pd.DataFrame({
            'number': [9999], 'name': ['Station Test'], 'address': ['123 Rue du Test'],
            'available_bikes': [10], 'available_bike_stands': [5], 'status': ['OPEN'],
            'position_lat': [43.6], 'position_lon': [1.44], 
            'last_updated': [pd.to_datetime('now', utc=True)],
            'metro_stations_nearby_500m': [1], 'bus_stops_nearby_200m': [3]
        })
        
        # 3. Lancer le chargement et la vérification
        load_to_database(dummy_df)
        verify_data()
    else:
        print("\n❌ Échec du test de connexion. Corrigez les paramètres dans DATABASE_CONFIG.")
    
    print("\n✅ Test de 'load_data.py' terminé.")