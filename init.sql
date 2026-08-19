-- Création de la table pour les activités sportives (Strava-like)
CREATE TABLE activites_sportives (
    id SERIAL PRIMARY KEY,
    id_salarie INTEGER NOT NULL,
    date_debut TIMESTAMP NOT NULL,
    sport_type VARCHAR(50) NOT NULL,
    distance_m INTEGER, -- Peut être NULL (ex: Escalade)
    temps_ecoule_s INTEGER NOT NULL,
    commentaire TEXT
);

-- Note : J'utilise 'temps_ecoule_s' comme dans l'exemple de la table du PDF, 
-- ce qui permet de déduire la 'Date de fin' demandée plus bas dans l'énoncé.