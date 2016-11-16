# classifieur_exoplanetes
Classifieur d’exoplanètes

# Intro:


**Objectif :** réaliser un classifieur d’exoplanètes labellisées "**confirmée**" ou "**faux-positif**". 

**Contexte :** Les exoplanètes sont des planètes tournant autour d’autres étoiles que le Soleil. Leur étude permet de mieux comprendre comment s’est formé le système solaire, et une fraction d’entre elles pourrait être propices au développement de la vie extra-terrestre ! Leur détection se fait en deux temps :
- Un satellite (Kepler) observe les étoiles et repère celles dont la courbe de luminosité présente un "creux", ce qui pourrait indiquer qu’une planète est passée devant (une partie de la lumière émise par l’étoile étant alors occultée par le passage de la planète).  Cette méthode dite “de transit” permet de définir des exoplanètes candidates, et de déduire les caractéristiques qu’aurait la planète si elle existait vraiment (distance à son étoile, diamètre, forme de son orbite, etc).
- Il faut ensuite valider ou invalider les candidates en utilisant une autre méthode plus coûteuse, reposant sur des mesures de vitesses radiales de l’étoile. Les candidates sont alors classées en "**confirmed**" ou "**false-positive**".

Comme il y a environ 200 milliards d’étoiles dans notre galaxie, et donc potentiellement autant (voire davantage !) d’exoplanètes, leur détection doit être automatisée pour “passer à l’échelle”. La méthode des transits se fait déjà de façon automatique (plus de 22 million de courbes de luminosité enregistrées par Kepler), mais pas la confirmation des planètes candidates, d’où le classifieur automatique que nous allons construire.

**Données :** Les données sur les exoplanètes sont publiques et accessibles en ligne (http://exoplanetarchive.ipac.caltech.edu/index.html). Il y a déjà 3 388 exoplanètes confirmées et environ autant de faux-positifs, notre classifieur sera entraîné sur ces données. Il y a une exoplanète par ligne. La colonne des labels (ce que nous allons chercher à prédire) s'appelle "**koi_disposition**". Le contenu des colonnes du dataset est expliqué ici (http://exoplanetarchive.ipac.caltech.edu/docs/API_kepcandidate_columns.html). Le classifieur utilisera uniquement les informations venant des courbes de luminosité.

**Fonctionnement :**
- Cleaning des données sources, l'objectif est de faire en sorte que les données soient correctement formatées pour l’entraînement du modèle. Le preprocessing est réalisé via "**Job**"
- Création d'un modèle de classification entraîné sur les données qui ont été pré-traitées. Cette classification est réalisée via "**JobML**"
- Pour compiler le projet, se placer dans le répertoire du projet (**cd ~/classifieur_exoplanetes**).
Executer la commande : **sbt assembly**
- Pour executer les différents jobs, se placer dans le répertoire spark (**cd ~/spark-2.0.0-bin-hadoop2.6**). 
Exécuter la commande avec comme argument le chemin du fichier csv pré-traité : 
                        ./spark-submit
                        --class com.sparkProject.**JobML**
                        --master local ~/classifieur_exoplanetes/target/scala-2.11/**tp_spark-assembly-1.0.jar**
                        ~/classifieur_exoplanetes/output_exoplanetes.csv/**part-r-00000-b103385c-202b-4e21-a0ce-e8195147c9bf.csv**

