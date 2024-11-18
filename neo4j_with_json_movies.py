import json
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import time


def welcome():
    print("""
__        __         _                                       _____         
\ \      / /   ___  | |   ___    ___    _ __ ___     ___    |_   _|   ___  
 \ \ /\ / /   / _ \ | |  / __|  / _ \  | '_ ` _ \   / _ \     | |    / _ \ 
  \ V  V /   |  __/ | | | (__  | (_) | | | | | | | |  __/     | |   | (_) |
   \_/\_/     \___| |_|  \___|  \___/  |_| |_| |_|  \___|     |_|    \___/ 
                                                                           
 _   _                  _  _         _                                     
| \ | |   ___    ___   | || |       | |                                    
|  \| |  / _ \  / _ \  | || |_   _  | |                                    
| |\  | |  __/ | (_) | |__   _| | |_| |                                    
|_| \_|  \___|  \___/     |_|    \___/                                     
                                                                           
  ____   _                  ____    ____                                   
 / ___| (_)  _ __     ___  |  _ \  | __ )                                  
| |     | | | '_ \   / _ \ | | | | |  _ \                                  
| |___  | | | | | | |  __/ | |_| | | |_) |                                 
 \____| |_| |_| |_|  \___| |____/  |____/                                  
""")


def connect_to_neo4J_DB():
    load_dotenv("neo4j_with_json_movies.env")
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    global driver
    driver = GraphDatabase.driver(uri, auth=(user, password))
    print("Connection Sucessful!!!")
    

def main_menu_selection(updated_results):
    user_choice = input(
    """
    Please Select An Option: \n
    1 - Add A Movie To The DB \n
    2 - Ask A Question To CineGPT \n
    3 - Quit \n
    """)
    if user_choice not in ['1', '2', '3']:
        print("\nERROR ~~~ Invalid Choice!!! ~~~ ERROR\n")
        
    if user_choice == '1':
        add_movie_to_db()
        print("Updating the DB Stats")
        updated_results = load_neo4j_stats()
        print("Sucessfully Updated!!!")
    if user_choice == '2':
        CineGPT(updated_results)
    if user_choice == '3':
        driver.close()
        return True


def add_movie_to_db():
    film_array = user_movie_input()

    error_count = 0
    total_count = len(film_array)
    films_with_error = []
    batch_data = []

    overall_start = time.time()
    for film in film_array:
        film_start = time.time()
        batch_data_start = time.time()

        construct_url_start = time.time()
        complete_url = construct_url(film)
        construct_url_end = time.time()
        print(f"URL Construction Time: {construct_url_end - construct_url_start:2f} Seconds")
        
        json_time_start = time.time()
        json_response = requests.get(complete_url)

        if json_response.status_code == 200:
            movie_data = json_response.json()
        else:
            error_count += 1
            films_with_error.append(film)
            print(f"ERROR ~~~ JSON Code: {json_response.status_code} ~~~ ERROR\n")
            continue

        json_time_end = time.time()
        print(f"JSON Load Time: {json_time_end - json_time_start:.2f} Seconds")

        try:
            movie = movie_data["Title"]
            director = movie_data["Director"]
            movie_release_year = movie_data["Year"]
            country_of_origin = movie_data["Country"].split(", ")[0]
            movie_rating = movie_data["Rated"]
            imdb_score = movie_data["imdbRating"]
            rotten_tomato_score = next((rating["Value"] for rating in movie_data["Ratings"] if rating["Source"] == "Rotten Tomatoes"), "N/A")
            metacritic_score = movie_data["Metascore"]
            box_office = movie_data["BoxOffice"]
            imdb_id = movie_data["imdbID"]
            plot = movie_data["Plot"]
            awards = movie_data["Awards"]
            actor_list = movie_data["Actors"].split(", ")
            genre_list = movie_data["Genre"].split(", ")
            language_list = movie_data["Language"].split(", ")
        except:
            error_count += 1
            films_with_error.append(film)
            print("\nERROR ~~~ Failed To Retrieve JSON ~~~ ERROR\n")
            continue

        batch_data.append({
                "movie": movie,
                "director": director,
                "movie_release_year": movie_release_year,
                "country_of_origin": country_of_origin,
                "movie_rating": movie_rating,
                "imdb_score": imdb_score,
                "rotten_tomato_score": rotten_tomato_score,
                "metacritic_score": metacritic_score,
                "box_office": box_office,
                "imdb_id": imdb_id,
                "plot": plot,
                "awards": awards,
                "actor_list": actor_list,
                "genre_list": genre_list,
                "language_list": language_list
            })
        
        film_end_time = time.time()
        print(f"\nFilm Retrieval Time: {film_end_time-film_start:2f} Seconds\n")
        
        if len(batch_data) >= 10:
            with driver.session() as session:
                session.execute_write(batch_insert, batch_data)
                batch_data_end = time.time()
                print(f"\nBatch of 10 Movies Inserted, Time: {batch_data_end-batch_data_start:.2f} Seconds\n")
                batch_data.clear()

    if batch_data:
        with driver.session() as session:
                session.execute_write(batch_insert, batch_data)
                batch_data_end = time.time()
                print(f"\nBatch of Movies Inserted, Time: {batch_data_end-batch_data_start:.2f} Seconds\n")
                batch_data.clear()

    overall_end = time.time()
    print(f"Overall Time: {overall_end - overall_start:.2f} Seconds\n")
    print(f"Sucess Rate: {((total_count - error_count) / total_count) * 100:.2f}%\n")
    print(f"Failed to Add These Movies: {films_with_error}\n")


def user_movie_input():
    film_names = input("Enter the name of the film you want to add or comma seperated values for the titles: \n\n")
    film_array = [film.strip() for film in film_names.split(',')]
    return film_array


def construct_url(title: str):
    api_key = os.getenv("OMDB_API_KEY")
    url = f"https://www.omdbapi.com/?t={title}&apikey={api_key}"
    return url


def batch_insert(tx, batch_data):
    query = """
        UNWIND $batch_data AS film
        MERGE (m:Movie {name: film.movie})
        MERGE (d:Director {name: film.director})
        MERGE (ry:ReleaseYear {year: film.movie_release_year})
        MERGE (coo:CountryOfOrigin {name: film.country_of_origin})
        MERGE (mr:MovieRating {name: film.movie_rating})

        MERGE (m)-[:DIRECTED_BY]->(d)
        MERGE (m)-[:RELEASE_YEAR]->(ry)
        MERGE (m)-[:COUNTRY_OF_ORIGIN]->(coo)
        MERGE (m)-[:MOVIE_RATING]->(mr)

        SET m.IMDB_RATING = film.imdb_score
        SET m.ROTTEN_TOMATOES = film.rotten_tomato_score
        SET m.METACRITIC_SCORE = film.metacritic_score
        SET m.BOX_OFFICE = film.box_office
        SET m.IMDB_ID = film.imdb_id
        SET m.PLOT = film.plot
        SET m.AWARDS = film.awards

        WITH m, film
        UNWIND film.actor_list AS actor
        MERGE (a:Actor {name: actor})
        MERGE (a)-[:ACTED_IN]->(m)

        WITH m, film
        UNWIND film.genre_list AS genre
        MERGE (g:Genre {name: genre})
        MERGE (m)-[:HAS_GENRE]->(g)

        WITH m, film
        UNWIND film.language_list AS language
        MERGE (l:Language {name: language})
        MERGE (m)-[:HAS_LANGUAGE]->(l)
    """
    tx.run(query, batch_data=batch_data)



def CineGPT(updated_results):
    get_neo4j_stats(updated_results)

def load_neo4j_stats():
    query = """
    MATCH (n) WITH COUNT(n) AS totalNodes
    MATCH (m:Movie) WITH totalNodes, COUNT(m) AS totalMovies
    MATCH (a:Actor) WITH totalNodes, totalMovies, COUNT(a) AS totalActors
    MATCH (g:Genre) WITH totalNodes, totalMovies, totalActors, COUNT(g) AS totalGenres
    MATCH (ry:ReleaseYear) WITH totalNodes, totalMovies, totalActors, totalGenres, COUNT(ry) AS totalYears
    MATCH (l:Language) WITH totalNodes, totalMovies, totalActors, totalGenres, totalYears, COUNT(l) AS totalLanguages
    MATCH (coo:CountryOfOrigin)
    RETURN totalNodes, totalMovies, totalActors, totalGenres, totalYears, totalLanguages, COUNT(coo) AS totalCountries
    """

    with driver.session() as session:
        result = session.run(query)
        return result.single()

def get_neo4j_stats(result):
    total_nodes = result["totalNodes"]
    total_movies = result["totalMovies"]
    total_actors = result["totalActors"]
    total_genres = result["totalGenres"]
    total_years = result["totalYears"]
    total_languages = result["totalLanguages"]
    total_countries = result["totalCountries"]
    print(f"""\nLoaded {total_nodes} Nodes And {total_movies} Movies. \n
    Also Loaded: \n
          {total_actors} Actors
          {total_genres} Genres
          {total_years} Years
          {total_languages} Languages
          {total_countries} Countries
""")


def main():
    welcome()
    print("Connecting to DB)")
    connect_to_neo4J_DB()
    print("Loading DB")
    updated_results = load_neo4j_stats()
    print("DB Loaded!!!")
    while True:
        if main_menu_selection(updated_results):
            break
        
if __name__ == "__main__":
    main()
