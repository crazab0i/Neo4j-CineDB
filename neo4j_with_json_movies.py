import json
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

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

def main_menu_selection():
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
    if user_choice == '2':
        pass
    if user_choice == '3':
        return True


def add_movie_to_db():
    film_name = user_movie_input()

    if movie_already_in_DB(film_name):
        print(f"The movie {film_name} already exists in the database.")

    complete_url = construct_url(film_name)
    json_response = requests.get(complete_url)

    if json_response.status_code == 200:
        movie_data = json_response.json()
    else:
        print(f"Error ~~~ JSON Code: {json_response.status_code()} ~~~ ERROR")

    query = """
        MERGE (m:Movie {name: $movie})
        MERGE (d:Director {name: $director})
        MERGE (ry:ReleaseYear {year: $movie_release_year})
        MERGE (coo:CountryOfOrigin {name: $country_of_origin})
        MERGE (mr:MovieRating {name: $movie_rating})

        MERGE (m)-[:DIRECTED_BY]->(d)
        MERGE (m)-[:RELEASE_YEAR]->(ry)
        MERGE (m)-[:COUNTRY_OF_ORIGIN]->(coo)
        MERGE (m)-[:MOVIE_RATING]->(mr)
        

        SET (m).IMDB_RATING = $imdb_score
        SET (m).ROTTEN_TOMATOES = $rotten_tomato_score
        SET (m).METACRITC_SCORE = $metacritic_score
        SET (m).BOX_OFFICE = $box_office
        SET (m).IMDB_ID = $imdb_id
        SET (m).PLOT = $plot
        SET (m).AWARDS = $awards

        WITH m
        UNWIND $actor_list AS actor
        MERGE (a:Actor {name: actor})
        MERGE (a)-[:ACTED_IN]->(m)

        WITH m
        UNWIND $genre_list AS genre
        MERGE (g:Genre {name: genre})
        MERGE (m)-[:HAS_GENRE]->(g)

        WITH m
        UNWIND $language_list AS language
        MERGE (l:Language {name: language})
        MERGE (m)-[:HAS_LANGUAGE]->(l)
    """
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
    
    with driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(query, movie=movie, director=director, movie_release_year=movie_release_year, country_of_origin=country_of_origin, movie_rating=movie_rating, imdb_score=imdb_score, rotten_tomato_score=rotten_tomato_score, 
           metacritic_score=metacritic_score, box_office=box_office, imdb_id=imdb_id, plot=plot, awards=awards, actor_list=actor_list, genre_list=genre_list, language_list=language_list)
        )
        print(f"Sucessfully Added Movie: {film_name}")

def user_movie_input():
    film_name = input("Enter the name of the film you want to add: \n\n")
    return film_name

def movie_already_in_DB(film_name):
    query = """
        MATCH (m: Movie {name: $title})
        RETURN COUNT(m) > 0 AS exists
        """
    with driver.session() as session:
        result = session.run(query, title=film_name)
        return result.single()["exists"]
    
def construct_url(title: str):
    api_key = os.getenv("OMDB_API_KEY")
    url = f"https://www.omdbapi.com/?t={title}&apikey={api_key}"
    return url


def main():
    welcome()
    connect_to_neo4J_DB()
    while True:
        if main_menu_selection():
            break
        
        


if __name__ == "__main__":
    main()
