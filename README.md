# Catalogue de Données - Projet ETL avec Azure Databricks

## Description des Données

### Données Originales
Les données originales utilisées dans ce projet comprennent les champs suivants :
- Date
- TransportType
- Route
- DepartureTime
- ArrivalTime
- Passagers
- DepartureStation
- ArrivalStation
- Retard

Ces données proviennent de la société X et servent de base pour l'analyse et la transformation ultérieures.

### Données Transformées
Les données ont été transformées en trois tables distinctes :
1. **Table Principale**
   - Champs inclus : Date, TransportType, Route, DepartureTime, ArrivalTime, Passagers, DepartureStation, ArrivalStation, Delay, Year, Month, Day, DelayCategory, Duration
   - Ces données sont prêtes pour les requêtes et les rapports avancés, avec des informations détaillées sur les retards, la durée des voyages, etc.

2. **Table d'Analyse des Itinéraires**
   - Champs inclus : Route, AvgPassengers, AvgDelay, Trips
   - Cette table fournit des statistiques agrégées par itinéraire, y compris le retard moyen, le nombre moyen de passagers et le nombre total de voyages.

3. **Table d'Analyse des Heures de Pointe**
   - Champs inclus : DepartureHour, AvgPassengers, Peak_hour
   - Cette table identifie les heures de pointe en fonction du nombre de passagers et des retards.

## Transformations

Toutes les transformations ont été appliquées aux données brutes conformément aux spécifications du projet. Les principales transformations comprennent :
- Extraction de l'année, du mois, du jour et du jour de la semaine à partir de la date.
- Calcul de la durée de chaque voyage en soustrayant l'heure de départ de l'heure d'arrivée.
- Catégorisation des retards en groupes tels que 'Pas de Retard', 'Retard Court' (1-10 minutes), 'Retard Moyen' (11-20 minutes) et 'Long Retard' (>20 minutes).
- Identification des heures de pointe en fonction du nombre de passagers.
- Calcul du retard moyen, du nombre moyen de passagers et du nombre total de voyages pour chaque itinéraire.

## Lignage des Données

Les données utilisées dans ce projet proviennent de la société X et ont été traitées à l'aide d'Azure Databricks. Le lien de lignage des données est le suivant : "Données provenant de la société X et traitées à l'aide d'Azure Databricks."

## Directives d'Utilisation

Les données transformées peuvent être utilisées pour divers cas d'utilisation, notamment :
- Analyse des performances de transport, y compris les retards et la durée des voyages.
- Optimisation des itinéraires en fonction des statistiques d'itinéraire.
- Planification des heures de pointe pour améliorer la gestion des ressources.
- Suivi des tendances des passagers pour ajuster les services de transport.

Il est important de prendre en compte les précautions suivantes lors de l'utilisation des données :
- Assurez-vous de comprendre la signification des catégories de retard pour une interprétation correcte.
- Vérifiez les données en fonction des besoins spécifiques de votre cas d'utilisation, car elles peuvent être soumises à des mises à jour périodiques.

# Automatisation des Politiques de Conservation

## Archivage des Données
La politique de conservation des données consiste à déplacer les fichiers les plus anciens vers une archive à des intervalles réguliers. Dans ce projet, nous avons configuré cette politique pour déplacer les fichiers vers une archive toutes les 2 minutes (à titre de test). Le processus d'archivage est automatisé et comprend les étapes suivantes :
- Identifie les fichiers les plus anciens dans le répertoire de données.
- Déplace ces fichiers vers un répertoire ou un conteneur d'archive spécifié.

# Génération de Données à Intervalle de Lots (Batch Intervals)

Des données supplémentaires sont générées à intervalles de lots à l'aide du script joint à ce brief. Chaque fichier CSV généré représente de nouvelles données pour une période spécifique de la journée ( 2 fichiers par batch), ce qui permet d'enrichir les données existantes.

# Batch Processing

Une fois les données collectées dans la journée, elles sont traitées selon le Job de traitement automatisé. Les nouveaux fichiers résultants sont enregistrés dans le dossier traité dans le datalake. Un job est créé pour exécuter le notebook de traitement. Ce Job peut être planifié pour s'exécuter à des intervalles spécifiques, par exemple, quotidiens ou hebdomadaires, afin de maintenir les données à jour.
