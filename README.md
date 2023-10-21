## Objectifs du projet

L’objectif est de rapatrier des données qui compilent des informations de diffusions de nouveaux épisodes de séries TV, pour ensuite les stocker de différentes façons et finalement faire quelques manipulations algorithmiques sur ces données.

## Pour Clonez le project

```
git clone https://github.com/arthurferreira64/eval.git
```

## Version de python

Pour se projet nous avons utilisé la version 10.0 de python avec un environnement virtuel

## Utilisation

Pour faire fonctionner le projet il faut décommenter les fonctions que l'on souhaite utiliser

## Colaborateur

Le projet à été fait par Arthur Ferreira Macias et Tom Lebrun

## Liste des fonctions :

scraper pour récuperer les données :
```python
data_from_scraping = scraping_data()
```
enregistrement des données du scraping dans le CSV :
```python
save_in_csv(data_from_scraping)
```
lire les données du CSV :
```python
data_from_csv = read_csv()
```
Enregistrement des données du CSV dans les bases de données :
```python
save_episodes_in_bdd(data_from_csv)
save_episodes_in_dist_bdd(data_from_csv)
```
Sélectionner les données de la table postgres pour vérifier que les données y sont présentes :
```python
select_episodes_from_postgres()
```
Obtenir un top 3 des chaînes avec le plus grand nombre d'épisodes en octobre :
```python
get_top_three_channels_october_episodes()
```
Obtenir un top 3 des pays avec le plus grand nombre d'épisodes en Octobre :
```python
get_top_three_countries_october_episodes()
```
Obtenir un top 10 des mots les plus présents dans les noms des séries :
```python
get_top_ten_common_words_in_series_names()
```
Scraper pour récupérer le temps de chaque épisode :
```python
episodes_duree = scraping_data_time()
```
Enregistrement de la durée des épisodes dans les BDD :
```python
save_time_in_bdd(episodes_duree)
save_time_in_dist_bdd(episodes_duree)
```
Trouver la chaîne avec le plus grand nombre de jours consécutifs :
```python
find_channel_with_longest_consecutive_days()
```
## Chaînes les plus actives

Les trois chaînes qui ont diffusé le plus d'épisodes sont :
```
1. Netflix à 109 épisodes
```
```
2.  Disney+ à 30 épisodes
```
```
4. Prime Video à 27 épisodes
```
## Pays les plus actifs

Les trois pays qui ont diffusé le plus d'épisodes sont :
```
1. Etats-Unis à 353 épisodes
```
```
2. France à 76 épisodes
```
```
3. Canada à 63 épisodes
```

## Les 10 mots les plus présents dans les noms des séries

Quels sont les 10 mots les plus présents dans les noms des séries :
```
1. The appelé 66 fois
```
```
2. of appelé 31 fois
```
```
3. de appelé 24 fois
```
```
4. (2023) appelé 19 fois
```
```
5. Pacto appelé 18 fois
```
```
6. Silencio 18 fois
```
```
7. Les appelé 17 fois
```
```
8. the appelé 16 fois
```
```
9. (UK) appelé 12 fois
```
```
10. Everything appelé 11 fois
```
