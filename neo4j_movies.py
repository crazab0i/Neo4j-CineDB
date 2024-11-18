from neo4j import GraphDatabase
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "12345678")


def main():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print("Connection Sucessful")
    
    while True:
        movie = input("\nEnter a movie to add: ")
        director = input ("\nEnter the name of the director: ")
        actor_string = input("\nEnter the list of actors seperated by commas: \n")
        actor_list = [actor for actor in actor_string.split(',')]
        movie_release_year = input("\nEnter the release year for this movie: ")
        country_of_origin = input("\nEnter the country of origin for this film: ")
        movie_rating = input("\nEnter the movie rating: ")
        movie_score = input("\nEnter the IMDB rating: ")
        print(f"""
            Movie: {movie}\n
            Director: {director}
            List of Actors: {actor_list}
            Movie Release Year: {movie_release_year}\n
            Country of Origin: {country_of_origin}\n
            Movie Rating: {movie_rating}
            IMDB Score: {movie_score}
            """)

        want_to_add = input("\nDo you want to add to the DB? (Y/N) ")
        if want_to_add.upper() == 'Y':
            with driver.session() as session:
                    session.execute_write(
                        add_movie_to_db,
                        movie,
                        director,
                        actor_list,
                        movie_release_year,
                        country_of_origin,
                        movie_rating,
                        movie_score
                    )
            print("Data successfully added to Neo4j!")
        
        break_loop = input("\nDo you want to exit the program? (Y/N) ")
        if break_loop.upper() == 'Y':
             print("Exiting program!")
             break

def add_movie_to_db(tx, movie, director, actor_list, movie_release_year, country_of_origin, movie_rating, movie_score):
    
    query = """
    MERGE (m:Movie {name: $movie})
    MERGE (d:Director {name: $director})
    MERGE (ry:ReleaseYear {year: $movie_release_year})
    MERGE (coo:CountryOfOrigin {name: $country_of_origin})
    MERGE (mr:MovieRating {name: $movie_rating})
    
    MERGE (m)-[:DIRECTED_BY]->(d)
    MERGE (m)-[:RELEASE_YEAR]->(ry)
    MERGE (m)-[:COUNTRY_OF_ORIGIN]->(coo)
    MERGE (m)-[:RATED_AS]->(mr)
    SET (m).IMDB_Rating = $movie_score

    WITH m
    UNWIND $actor_list AS actor
    MERGE (a:Actor {name: actor})
    MERGE (a)-[:ACTED_IN]->(m)  
    """
    
    tx.run(query, movie=movie, director=director, actor_list=actor_list, movie_release_year=movie_release_year, country_of_origin=country_of_origin, movie_rating=movie_rating, movie_score=movie_score)

if __name__ == "__main__":
    main()