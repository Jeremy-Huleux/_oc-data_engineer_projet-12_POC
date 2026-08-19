import os
import pandas as pd
import googlemaps
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Chargement des variables d'environnement (Sécurité)
load_dotenv()
API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
gmaps = googlemaps.Client(key=API_KEY)

COMPANY_ADDRESS = "1362 Av. des Platanes, 34970 Lattes"

def validate_commute(df_rh):
    """Vérifie la cohérence des déclarations de déplacement."""
    print("--- Début de la validation des distances ---")
    anomalies = []
    
    for index, row in df_rh.iterrows():
        mode = row['Moyen de déplacement']
        address = row['Adresse du domicile']
        
        # On ne vérifie que les modes sportifs
        if mode in ['Marche/running', 'Vélo/Trottinette/Autres']:
            try:
                # Appel à l'API Google Maps
                result = gmaps.distance_matrix(address, COMPANY_ADDRESS, mode='walking')
                # Récupération de la distance en mètres, puis conversion en km
                distance_m = result['rows'][0]['elements'][0]['distance']['value']
                distance_km = distance_m / 1000
                
                # Vérification des règles métier
                if mode == 'Marche/running' and distance_km > 15:
                    anomalies.append(f"Anomalie : {row['Nom']} {row['Prénom']} - Marche ({distance_km} km > 15 km)")
                elif mode == 'Vélo/Trottinette/Autres' and distance_km > 25:
                    anomalies.append(f"Anomalie : {row['Nom']} {row['Prénom']} - Vélo ({distance_km} km > 25 km)")
                    
            except Exception as e:
                print(f"Erreur API pour l'adresse {address}: {e}")
                
    if anomalies:
        print("Les anomalies suivantes ont été détectées :")
        for a in anomalies:
            print(a)
    else:
        print("Aucune anomalie détectée dans les déclarations.")
    print("------------------------------------------\n")

def generate_strava_data(df_rh, df_sport, num_records=2000):
    """Génère 12 mois de données fictives."""
    print("--- Génération des données historiques ---")
    
    # On récupère la liste des salariés qui font du sport
    sportifs = df_sport.dropna(subset=["Pratique d'un sport"])['ID salarié'].tolist()
    
    data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365) # Historique de 12 mois
    
    sports_list = ['Course à pied', 'Vélo', 'Natation', 'Randonnée', 'Escalade']
    comments = ["Super sortie !", "Dur aujourd'hui...", "Reprise du sport :)", "", "Nouveau record perso !"]
    
    for _ in range(num_records):
        # Sélection aléatoire d'un salarié parmi ceux qui font du sport
        id_salarie = random.choice(sportifs) if sportifs else random.choice(df_rh['ID salarié'].tolist())
        
        # Date aléatoire dans l'année écoulée
        random_days = random.randrange((end_date - start_date).days)
        date_debut = start_date + timedelta(days=random_days, hours=random.randint(6, 19))
        
        sport = random.choice(sports_list)
        
        # Logique de génération cohérente
        if sport == 'Escalade':
            distance = None # Pas de distance pour l'escalade
            temps = random.randint(3600, 7200) # 1h à 2h
        elif sport == 'Course à pied':
            distance = random.randint(3000, 15000) # 3km à 15km
            temps = int(distance * (random.uniform(5, 7) * 60 / 1000)) # Allure entre 5 et 7 min/km
        else:
            distance = random.randint(10000, 50000)
            temps = random.randint(1800, 7200)
            
        data.append({
            'id_salarie': id_salarie,
            'date_debut': date_debut,
            'sport_type': sport,
            'distance_m': distance,
            'temps_ecoule_s': temps,
            'commentaire': random.choice(comments)
        })
        
    df_generated = pd.DataFrame(data)
    print(f"{len(df_generated)} lignes générées avec succès.")
    return df_generated

def load_to_postgres(df):
    """Charge le DataFrame dans la base PostgreSQL."""
    print("--- Chargement dans PostgreSQL ---")
    
    # Création de la chaîne de connexion SQLAlchemy
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url)
    
    try:
        # if_exists='append' permet d'ajouter les données à la table existante
        # index=False évite d'insérer l'index du DataFrame Pandas
        df.to_sql('activites_sportives', engine, if_exists='append', index=False)
        print("Chargement terminé avec succès dans la table 'activites_sportives'.")
    except Exception as e:
        print(f"Erreur lors de l'insertion : {e}")

if __name__ == "__main__":
    # 1. Extraction (Lecture des fichiers)
    # Assure-toi que les fichiers sont dans le même dossier que le script
    df_rh = pd.read_excel("Données+RH.xlsx")
    df_sport = pd.read_excel("Données+Sportive.xlsx")
    
    # 2. Validation
    validate_commute(df_rh)
    
    # 3. Génération
    df_gen = generate_strava_data(df_rh, df_sport, num_records=3000)
    
    # 4. Chargement
    load_to_postgres(df_gen)