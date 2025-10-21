"""
Module d'Extraction des Données VéloToulouse (JCDecaux)
"""

import pandas as pd
import requests
import os
import sys

# Clé API JCDecaux 

API_KEY = "ab168389738c556afb9ff7bd969c01c47b8c22f8"
CONTRACT_NAME = "toulouse"

def extract_stations():
    """
    Extrait les données des stations de vélos depuis l'API JCDecaux.
    
    Retourne:
        pandas.DataFrame: Données des stations avec leur occupation actuelle.
                         Retourne un DataFrame vide en cas d'erreur.
    """
    print("🌐 Récupération des données stations depuis l'API JCDecaux...")
    
    url = f"https://api.jcdecaux.com/vls/v1/stations?contract={CONTRACT_NAME}&apiKey={API_KEY}"
    
    try:
        print("Appel API en cours...")
        # Timeout augmenté pour les connexions lentes
        response = requests.get(url, timeout=30) 
        
        # Lève une exception si le statut n'est pas 200 (OK)
        response.raise_for_status() 
        
        data = response.json()
        
        # Convertit les données JSON en DataFrame pandas
        df = pd.DataFrame(data)
        
       

        print(f"✅ {len(df)} stations trouvées.")
        return df
        
    except requests.exceptions.RequestException as e:
        # Gère les erreurs réseau (timeout, DNS, etc.)
        print(f"❌ Erreur réseau lors de la récupération des stations: {e}", file=sys.stderr)
        return pd.DataFrame() # Retourne un DataFrame vide
    except Exception as e:
        # Gère les autres erreurs (JSON invalide, etc.)
        print(f"❌ Erreur lors du traitement des données stations: {e}", file=sys.stderr)
        return pd.DataFrame() # Retourne un DataFrame vide

def test_api_connection():
    """
    Vérifie si l'API JCDecaux est accessible.
    """
    print("\n🔍 Test de la connexion à l'API...")
    url = f"https://api.jcdecaux.com/vls/v1/stations?contract={CONTRACT_NAME}&apiKey={API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print("✅ Connexion API réussie.")
            return True
        else:
            print(f"⚠️ Connexion API échouée. Statut: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Connexion API échouée: {e}")
        return False

# Ce bloc est exécuté seulement si on lance ce fichier directement
# (par exemple: python src/extract_data.py)
if __name__ == "__main__":
    print("--- Test du module d'extraction ---")
    
    if test_api_connection():
        stations_data = extract_stations()
        if not stations_data.empty:
            print(f"\nExtraction réussie. DataFrame shape: {stations_data.shape}")
            print("Aperçu des données :")
            print(stations_data.head())
    else:
        print("\nSkipping extraction test due to API connection issues.")