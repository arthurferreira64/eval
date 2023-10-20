import sqlite3
import psycopg2
import pandas as pd


class Database:

    def __init__(self):
        self.URL_DB = "postgres://cours_pytho_8210:B5vjLg-yolNLvEX_o_FK@cours-pytho-8210.postgresql.a.osc-fr1.scalingo-dbs.com:33673/cours_pytho_8210?sslmode=prefer"

    def execute_query_and_get_dataframe(self, query):
        conn = None
        try:
            conn = psycopg2.connect(self.URL_DB)
            df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            print(e)
        finally:
            if conn is not None:
                conn.close()

    def get_episodes_for_NBC(self):
        try:
            conn = psycopg2.connect(self.URL_DB)
            cursor = conn.cursor()

            query = "SELECT id, episode_url_relative FROM episode WHERE chaine_diffusion = 'NBC'"

            cursor.execute(query)

            results = cursor.fetchall()

            cursor.close()
            conn.close()

            return results

        except psycopg2.Error as e:
            print(e)
            return None

    def save_to_dist_bdd(self, episodes):
        try:
            conn = psycopg2.connect(self.URL_DB)
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

    def save_time_in_dist_bdd(self, duration_data):
        try:
            conn = psycopg2.connect(self.URL_DB)
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

            cursor.executemany('''
            INSERT INTO duration (duration, episode_id)
            VALUES (%s, %s)
        ''', duration_data)

            conn.commit()

        except psycopg2.Error as e:
            print(e)
        finally:
            if conn:
                cursor.close()
                conn.close()
                print("Connexion fermée.")

    def select_episodes_from_postgres(self):
        try:
            conn = psycopg2.connect(self.URL_DB)
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

    def save_to_bdd(self, episodes):
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

    def get_top_ten_common_words_in_series_names(self, url):
        query_words = """
            SELECT REGEXP_SPLIT_TO_TABLE(nom_serie, E'\\\\s+') AS mots, COUNT(*) as nb_occurrences
            FROM episode
            GROUP BY mots
            ORDER BY nb_occurrences DESC
            LIMIT 10
        """
        df_words = self.execute_query_and_get_dataframe(query_words, url)

        print("\nLes 10 mots les plus présents dans les noms des séries :")
        print(df_words)

    def get_top_three_channels_october_episodes(self, url):
        query_channel = """
            SELECT chaine_diffusion, COUNT(*) as nb_episodes
            FROM episode
            WHERE date_diffusion LIKE '%%-10-%%'
            GROUP BY chaine_diffusion
            ORDER BY nb_episodes DESC
            LIMIT 3
        """

        df_channel = self.execute_query_and_get_dataframe(query_channel, url)

        print("Chaînes avec le plus grand nombre d'épisodes en Octobre :")
        print(df_channel)

    def get_top_three_countries_october_episodes(self, url):
        query_country = """
            SELECT pays_origine, COUNT(*) as nb_episodes
            FROM episode
            WHERE date_diffusion LIKE '%%-10-%%'
            GROUP BY pays_origine
            ORDER BY nb_episodes DESC
            LIMIT 3
        """
        df_country = self.execute_query_and_get_dataframe(query_country, url)

        print("\nPays avec le plus grand nombre d'épisodes en Octobre :")
        print(df_country)

    def save_episodes_in_bdd(episodes):
        # Connexion à la base de données SQLite
        conn = sqlite3.connect('data/databases/database.db')

        # Création d'un curseur pour exécuter des requêtes
        cursor = conn.cursor()

        # Création de la table episode si elle n'existe pas
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

        # Insertion des données dans la base de données
        for episode in episodes:
            cursor.execute('''
                INSERT INTO episode (nom_serie, numero_episode, numero_saison, date_diffusion, pays_origine, chaine_diffusion, episode_url_relative)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (episode['nom_serie'], episode['numero_episode'], episode['numero_saison'], episode['date_diffusion'], episode['pays_origine'], episode['chaine_diffusion'], episode['episode_url_relative']))

        # Valider les changements
        conn.commit()

        # Fermer la connexion
        conn.close()
