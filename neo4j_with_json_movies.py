import json
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import time
from langchain_core.prompts import PromptTemplate
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
import re
import csv

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
    

def main_menu_selection(updated_results, debug):
    user_choice = input(
    """
    Please Select An Option: \n
    1 - Add A Movie To The DB \n
    2 - Ask A Question To CineGPT \n
    3 - Quit \n
    d - To Toggle Debug Mode\n
    """)
    if user_choice not in ['1', '2', '3', 'd']:
        print("\nERROR ~~~ Invalid Choice!!! ~~~ ERROR\n")

    if user_choice == 'd':
        debug = True
        print("Changed debug")

    if user_choice == '1':
        get_user_input_method()
        print("Updating the DB Stats")
        updated_results = load_neo4j_stats()
        print("Sucessfully Updated!!!")
    if user_choice == '2':
        CineGPT(updated_results, debug)
    
    if user_choice == '3':
        driver.close()
        return True

def get_user_input_method():
    user_input = input("""How Do You Want To Add To DB?:\n
    m - Manual Input\n
    c - CSV\n
    """)
    if user_input.lower() not in ['m', 'c']:
        print("ERROR ~~~ Incorrect Input ~~~ ERROR")
    if user_input == 'm':
        add_movie_to_db_manual()
    if user_input == 'c':
        add_movie_to_db_csv()

def add_movie_to_db_manual():
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
        print(f"Film Retrieval Time: {film_end_time-film_start:2f} Seconds\n")
        
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

def add_movie_to_db_csv():
    file_location_input = input("CSV File Location: \n")
    batch_size_input = input("Input Batch Size (default: 5000): \n")
    try:
        batch_size_input_int = int(batch_size_input)
    except:
        batch_size_input_int = 5000
    
    batch_data = []
    error_count = 0
    total_count = 0
    films_with_error = []
    progress_file = "progress_file.txt"
    start_index = 0

    try:
        with open(progress_file, "r", encoding="utf-8") as progress_file_open:
            start_index = int(progress_file_open.read().strip())
        with open(file_location_input, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            overall_start = time.time()
            for index, row in enumerate(reader, start=1):
                if index <= start_index:
                    continue
                
                total_count += 1
                film_start = time.time()
                batch_data_start = time.time()
                if total_count >= batch_size_input_int:
                    break
                construct_url_start = time.time()
                complete_url = construct_url(row["title"])
                construct_url_end = time.time()
                print(f"URL Construction Time: {construct_url_end - construct_url_start:2f} Seconds")
                
                json_time_start = time.time()
                json_response = requests.get(complete_url)

                if json_response.status_code == 200:
                    movie_data = json_response.json()
                else:
                    error_count += 1
                    films_with_error.append(row["title"])
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
                    films_with_error.append(row["title"])
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
                print(f"Film Retrieval Time: {film_end_time-film_start:2f} Seconds\n")
                
                if len(batch_data) >= 20:
                    with driver.session() as session:
                        session.execute_write(batch_insert, batch_data)
                        batch_data_end = time.time()
                        print(f"\nBatch of 20 Movies Inserted, Time: {batch_data_end-batch_data_start:.2f} Seconds\n")
                        batch_data.clear()

            if batch_data:
                with driver.session() as session:
                        session.execute_write(batch_insert, batch_data)
                        batch_data_end = time.time()
                        print(f"\nBatch of Movies Inserted, Time: {batch_data_end-batch_data_start:.2f} Seconds\n")
                        batch_data.clear()

            with open(progress_file, "w", encoding="utf-8") as progress_file_write:
                progress_file_write.write(str(index))

            overall_end = time.time()
            print(f"Overall Time: {overall_end - overall_start:.2f} Seconds\n")
            print(f"Sucess Rate: {((total_count - error_count) / total_count) * 100:.2f}%\n")
            print(f"Failed to Add These Movies: {films_with_error}\n")
    except:
        print("ERROR ~~~ CANNOT READ CSV FILE ~~~ ERROR")
        with open(progress_file, "w", encoding="utf-8") as progress_file_write:
                progress_file_write.write(str(index))
    


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



def CineGPT(updated_results, debug):
    get_neo4j_stats(updated_results)
    model = load_langchain_api()
    user_cinegpt_input = input("What question would you like to ask CineGPT?: \n")
    cleaned_user_query = query_restructuring(user_cinegpt_input, model, debug)
    complete_output = query_db_and_create_output(cleaned_user_query, user_cinegpt_input, model, debug)
    print(f"\n{complete_output}")



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

def load_langchain_api():
    try:
        load_dotenv("langchain_api.env")
        os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_KEY")
        os.environ["LANGCHAIN_TRACING_V2"] = os.getenv("LANGCHAIN_TRACING")
        os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGCHAIN_API_KEY")
        os.environ["LANGCHAIN_ENDPOINT"] = os.getenv("LANGCHAIN_ENDPOINT")
        chat_model = ChatOpenAI(model="gpt-3.5-turbo", )
        print("Loaded AI Environment Sucessfully\n")
        return chat_model
    except:
        print("\nERROR ~~~ AI ENVIRONMENT FILE FAILURE ~~~ ERROR\n")
    
def query_restructuring(user_input, model, debug):
    restructured_query_template = ChatPromptTemplate([
        ("system", """
         You are a helpful assistant that rewrites user queries into Cypher queries for a Neo4j movie database. 
        Include both nodes, relationships, and relevant properties in the queries. Use a default limit of 10 if the user does not specify.
        Rewrites characters or titles that will fit actor names or imdb movie titles.
        
        - Node Types and Properties:
        - Movie: (name, IMDB_RATING, ROTTEN_TOMATOES, METACRITIC_SCORE, BOX_OFFICE, PLOT, AWARDS, IMDB_ID)
        - Actor: (name, age, gender)
        - Genre: (name)
        - Language: (name)
        - Director: (name)
        - CountryOfOrigin: (name)
        - ReleaseYear: (year) # Inequality with year requires quotes around it, eg ReleaseYear > "2020"
        - MovieRating: (name)

        - Relationship Types:
        - ACTED_IN
        - DIRECTED_BY
        - HAS_GENRE
        - RELEASE_YEAR
        - COUNTRY_OF_ORIGIN
        - MOVIE_RATING
        - HAS_LANGUAGE

        - Relationship Directions (Relations only go in one direction):
        - (a:Actor)-[:ACTED_IN]->(m:Movie)
        - (m:Movie)-[:DIRECTED_BY]->(d:Director) # This relationship must always be directed from Movie to Director
        - (m:Movie)-[:HAS_GENRE]->(g:Genre)
        - (m:Movie)-[:RELEASE_YEAR]->(ry:ReleaseYear)
        - (m:Movie)-[:COUNTRY_OF_ORIGIN]->(coo:CountryOfOrigin)
        - (m:movie)-[:MOVIE_RATING]->(r:MovieRating)
        - (m:Movie)-[r:HAS_LANGUAGE]->(l:Language)
         
        If you use EXISTS, use IS NOT NULL instead
        When querying properties, exclude values of "N/A"
        Output only the Cypher Query and nothing else please
        Since all properties are names you have to convert values like BOX_OFFICE are stored as comma seperated strings, e.g., "1,000,000"
        Comparisons with nodes or properties like ReleaseYear.name or .IMDB_RATING have to be enclosed with parenthesis, e.g., ReleaseYear > "2020", IMDB_RATING > "8.0"         
        EXAMPLES:
        1. user query: "What are the 10 highest rotten tomatoes and imdb christian bale movies"
        cypher query: MATCH (a:Actor)-[:ACTED_IN]->(m:Movie)
            WHERE a.name = "Christian Bale" 
            AND m.ROTTEN_TOMATOES IS NOT NULL 
            AND m.IMDB_RATING IS NOT NULL
            RETURN m.name AS Movie, m.ROTTEN_TOMATOES AS RottenTomatoes, m.IMDB_RATING AS IMDBRating
            ORDER BY m.ROTTEN_TOMATOES DESC, m.IMDB_RATING DESC
            LIMIT 10
         2. user query "How many movies did Christopher Noaln direct?"
         cypher query: 
            MATCH (d:Director {{name: "Christopher Nolan"}})<-[:DIRECTED_BY]-(m:Movie)
            RETURN COUNT(m) AS NumberOfMoviesDirected;
         3. user query "What are 3 movies from korea with the highest imdb rating?"
         cypher query:
            MATCH (m:Movie)-[:COUNTRY_OF_ORIGIN]->(:CountryOfOrigin {{name: "South Korea"}})
            WHERE m.IMDB_RATING IS NOT NULL AND m.IMDB_RATING <> "N/A"
            RETURN m.name AS Movie, m.IMDB_RATING AS IMDBRating
            ORDER BY m.IMDB_RATING DESC
            LIMIT 3
         4. user query "What are the 3 highest box office films from the united states in 2023?"
            MATCH (m:Movie)-[:COUNTRY_OF_ORIGIN]->(:CountryOfOrigin {{name: "United States"}})
            MATCH (m)-[:RELEASE_YEAR]->(ry:ReleaseYear {{year: "2023"}})
            WHERE m.BOX_OFFICE IS NOT NULL AND m.BOX_OFFICE <> "N/A"
            RETURN m.name AS Movie, m.BOX_OFFICE AS BoxOffice
            ORDER BY m.BOX_OFFICE DESC
            LIMIT 3
         """),
         ("human", "The query to convert is: {query}")])
    llm_chain = restructured_query_template | model
    restructured_query = llm_chain.invoke({"query": user_input})
    cleaned_query = re.sub(r'```cypher\n(.*?)\n```', r'\1', restructured_query.content, flags=re.DOTALL)
    print(f"\n{cleaned_query}\n")
    return cleaned_query

def query_db_and_create_output(cleaned_query, user_query, model, debug):
        try:
            with driver.session() as session:
                retrieved_data = session.run(cleaned_query)
                results = retrieved_data.data()
                if results is None:
                    print("No results found.")
        except:
            print("ERROR Querying")
        if debug:
            print(f"\n{results}\n")
        structured_final_output = ChatPromptTemplate([
            ("system", """
            You are a helpful assistant that writes answers to user questions using retrieved data from a database.
            """),
            ("human", "The original user query: {user_query} and the retrieved data: {retrieved_data}")
        ])
        try:
            llm_chain = structured_final_output | model
            final_output = llm_chain.invoke({"user_query": user_query, "retrieved_data": results})
            return final_output.content
        except:
            print("Error")


def main():
    welcome()
    print("Connecting to DB")
    connect_to_neo4J_DB()
    print("Loading DB")
    updated_results = load_neo4j_stats()
    print("DB Loaded!!!")
    debug = False
    while True:
        if main_menu_selection(updated_results, debug):
            break
        
if __name__ == "__main__":
    main()
