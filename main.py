import requests
from bs4 import BeautifulSoup
import csv
import sqlite3
import psycopg2
import pandas as pd
import time
import re
from collections import defaultdict


URL_DB = "postgres://cours_pytho_8210:B5vjLg-yolNLvEX_o_FK@cours-pytho-8210.postgresql.a.osc-fr1.scalingo-dbs.com:33673/cours_pytho_8210?sslmode=prefer"


# Partie 1: Manipulation des fichiers CSV


def read_csv():
    with open('data/files/episodes.csv', 'r', newline='', encoding='utf-8') as file:
        lines = file.readlines()

    data = []

    for line in lines:
        # Supprimer les retours à la ligne et diviser les valeurs séparées par des virgules
        values = line.strip().split(',')

        row_data = {
            "nom_serie": values[0],
            "numero_episode": values[1],
            "numero_saison": values[2],
            "date_diffusion": values[3],
            "pays_origine": values[4],
            "chaine_diffusion": values[5],
            "episode_url_relative": values[6],
        }

        data.append(row_data)

    return data


def save_in_csv(all_series):
    with open('data/files/episodes.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=all_series[0].keys())
        writer.writerows(all_series)

    # Partie 2: Connexion et manipulation des bases de données


def save_episodes_in_bdd(episodes):
    conn = sqlite3.connect('data/databases/database.db')

    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS episode (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom_serie TEXT,
            numero_episode INTEGER,
            numero_saison INTEGER,
            date_diffusion TEXT,
            pays_origine TEXT,
            chaine_diffusion TEXT,
            episode_url_relative TEXT
        )
    ''')

    for episode in episodes:
        cursor.execute('''
            INSERT INTO episode (nom_serie, numero_episode, numero_saison, date_diffusion, pays_origine, chaine_diffusion, episode_url_relative)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (episode['nom_serie'], episode['numero_episode'], episode['numero_saison'], episode['date_diffusion'], episode['pays_origine'], episode['chaine_diffusion'], episode['episode_url_relative']))

    conn.commit()
    conn.close()


def save_episodes_in_dist_bdd(episodes):

    try:
        conn = psycopg2.connect(URL_DB)
        print("Connexion réussie")

        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS episode (
                id SERIAL PRIMARY KEY,
                nom_serie TEXT,
                numero_episode INTEGER,
                numero_saison INTEGER,
                date_diffusion TEXT,
                pays_origine TEXT,
                chaine_diffusion TEXT,
                episode_url_relative TEXT
            )
        ''')

        for episode in episodes:
            cursor.execute('''
                INSERT INTO episode (nom_serie, numero_episode, numero_saison, date_diffusion, pays_origine, chaine_diffusion, episode_url_relative)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (episode['nom_serie'], episode['numero_episode'], episode['numero_saison'], episode['date_diffusion'], episode['pays_origine'], episode['chaine_diffusion'], episode['episode_url_relative']))

        conn.commit()

    except psycopg2.Error as e:
        print(e)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Connexion fermée.")


def save_time_in_bdd(duration_data):
    conn = sqlite3.connect('data/databases/database.db')

    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS duration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            duration INTEGER,
            episode_id INTEGER,
            FOREIGN KEY(episode_id) REFERENCES episode(id)
        )
    ''')

    cursor.executemany('''
        INSERT INTO duration (duration, episode_id)
        VALUES (?, ?)
    ''', duration_data)

    conn.commit()
    conn.close()


def save_time_in_dist_bdd(duration_data):
    try:
        conn = psycopg2.connect(URL_DB)
        print("Connexion réussie")

        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS duration (
            id SERIAL PRIMARY KEY,
            duration INTEGER,
            episode_id INTEGER,
            FOREIGN KEY(episode_id) REFERENCES episode(id)
        )
    ''')

        # Insertion des données dans la table duration
        cursor.executemany('''
        INSERT INTO duration (duration, episode_id)
        VALUES (%s, %s)
    ''', duration_data)

        conn.commit()

    except psycopg2.Error as e:
        print(e)
    finally:
        # Fermer le curseur et la connexion
        if conn:
            cursor.close()
            conn.close()
            print("Connexion fermée.")


def execute_query_and_get_dataframe(query):
    conn = None
    try:
        conn = psycopg2.connect(
            URL_DB)
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()


def select_episodes_from_postgres():
    try:
        conn = psycopg2.connect(URL_DB)
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM episode')
        rows = cursor.fetchall()

        return rows

    except psycopg2.Error as e:
        print(e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_episodes_for_apple():
    try:
        conn = psycopg2.connect(URL_DB)
        cursor = conn.cursor()

        query = "SELECT id, episode_url_relative FROM episode WHERE chaine_diffusion = 'Apple TV+'"

        cursor.execute(query)

        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return results

    except psycopg2.Error as e:
        print(e)
        return None

    # Partie 3: Web scraping


def scraping_data():
    url = "https://www.spin-off.fr/calendrier_des_series.html"

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Rechercher les balises <td> contenant les informations des séries
        series_cells = soup.find_all('td', class_='floatleftmobile')

        all_series = []
        for cell in series_cells:
            # Extraire les informations de chaque série
            series_info = cell.find_all('span', class_='calendrier_episodes')

            series_list = []
            for series in series_info:
                nom_serie = series.find('a').text
                numero_episode = series.find_all('a')[1].text.split(".")[1]
                numero_saison = series.find_all('a')[1].text.split(".")[0]

                div_tag = cell.find('div', class_=lambda x: x and (
                    'div_jourcourant' in x or 'div_jour' in x))
                date_diffusion = div_tag['id'].split(
                    '_')[1] if div_tag is not None else ""

                chaine = series.find_previous('img', alt=True)
                chaine_diffusion = chaine['alt']
                pays = chaine.find_previous('img', alt=True)
                pays_origine = pays['alt']

                # L'URL relative est dans le lien de la première série
                episode_link = series.find('a', class_='liens')['href']
                episode_url_relative = episode_link.split('/')[-1]

                obj = {}
                obj['nom_serie'] = nom_serie
                obj['numero_episode'] = numero_episode
                obj['numero_saison'] = numero_saison
                obj['date_diffusion'] = date_diffusion
                obj['pays_origine'] = pays_origine
                obj['chaine_diffusion'] = chaine_diffusion
                obj['episode_url_relative'] = episode_url_relative

                series_list.append(obj)

            all_series.append(series_list)

        all_series = [serie for sublist in all_series for serie in sublist]

        return all_series

    else:
        print("La requête a échoué avec le code", response.status_code)


def scraping_data_time():
    episodes_for_apple = get_episodes_for_apple()

    array = []
    for episode_for_apple in episodes_for_apple:

        url = f"https://www.spin-off.fr/{episode_for_apple[1]}"

        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            duree_episode_div = soup.find_all(
                'div', class_='episode_infos_episode_format')

            duree_episode = re.search(
                r'\d+', duree_episode_div[0].text).group()

            array.append((duree_episode, episode_for_apple[0]))
            time.sleep(2)
        else:
            print("La requête a échoué avec le code", response.status_code)
    return array

# Cette fonction ne fonctionne pas. On a essayé de faire quelque chose, mais cela ne fonctionne pas.
def find_channel_with_longest_consecutive_days():
    episodes = select_episodes_from_postgres()

   # Initialiser un dictionnaire pour suivre les jours consécutifs par chaîne de télévision
    channels = defaultdict(list)

    # Parcourir les données et enregistrer les jours consécutifs pour chaque chaîne de télévision
    for episode in episodes:
        _, _, _, _, date, _, channel_name, _ = episode
        channels[channel_name].append(date) if re.match(
            r'\d{2}-10-2023', date) and channels[channel_name] and date != channels[channel_name][-1] else channels.update({channel_name: [date]})

    # Identifier la chaîne de télévision avec le plus grand nombre de jours consécutifs
    max_consecutive_days = max(len(dates) for dates in channels.values())
    channel_with_max_days = next(channel for channel, dates in channels.items(
    ) if len(dates) == max_consecutive_days)
    print(
        f"La chaîne de télévision qui diffuse des épisodes pendant le plus grand nombre de jours consécutifs en octobre est : {channel_with_max_days}.")

    # Partie 4: Exécution de requêtes SQL


def get_top_ten_common_words_in_series_names():
    query_words = """
        SELECT REGEXP_SPLIT_TO_TABLE(nom_serie, E'\\\\s+') AS mots, COUNT(*) as nb_occurrences
        FROM episode
        GROUP BY mots
        ORDER BY nb_occurrences DESC
        LIMIT 10
    """

    df_words = execute_query_and_get_dataframe(query_words)

    print("\nLes 10 mots les plus présents dans les noms des séries :")
    print(df_words)


def get_top_three_channels_october_episodes():
    query_channel = """
        SELECT chaine_diffusion, COUNT(*) as nb_episodes
        FROM episode
        WHERE date_diffusion LIKE '%%-10-%%'
        GROUP BY chaine_diffusion
        ORDER BY nb_episodes DESC
        LIMIT 3
    """

    df_channel = execute_query_and_get_dataframe(
        query_channel)

    print("Chaînes avec le plus grand nombre d'épisodes en Octobre :")
    print(df_channel)


def get_top_three_countries_october_episodes():
    query_country = """
        SELECT pays_origine, COUNT(*) as nb_episodes
        FROM episode
        WHERE date_diffusion LIKE '%%-10-%%'
        GROUP BY pays_origine
        ORDER BY nb_episodes DESC
        LIMIT 3
    """
    df_country = execute_query_and_get_dataframe(query_country)

    print("\nPays avec le plus grand nombre d'épisodes en Octobre :")
    print(df_country)

    # Exécution des différentes fonctionnalités


# scraper pour récuperer les données
# data_from_scraping = scraping_data()

# enregistrement des données du scraping dans le CSV
# save_in_csv(data_from_scraping)

# lire les données du CSV
# data_from_csv = read_csv()

# Enregistrement des données du CSV dans les bases de données
# save_episodes_in_bdd(data_from_csv)
# save_episodes_in_dist_bdd(data_from_csv)

# Sélectionner les données de la table postgres pour vérifier que les données y sont présentes
# select_episodes_from_postgres()

# Obtenir un top 3 des chaînes avec le plus grand nombre d'épisodes en octobre
# get_top_three_channels_october_episodes()

# Obtenir un top 3 des pays avec le plus grand nombre d'épisodes en Octobre
# get_top_three_countries_october_episodes()

# Obtenir un top 10 des mots les plus présents dans les noms des séries
# get_top_ten_common_words_in_series_names()

# Scraper pour récupérer le temps de chaque épisode
# episodes_duree = scraping_data_time()

# Enregistrement de la durée des épisodes dans les BDD
# save_time_in_bdd(episodes_duree)
# save_time_in_dist_bdd(episodes_duree)

# Trouver la chaîne avec le plus grand nombre de jours consécutifs
# find_channel_with_longest_consecutive_days()
