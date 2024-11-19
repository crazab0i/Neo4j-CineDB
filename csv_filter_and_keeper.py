import pandas as pd

# Load your CSVs
existing_movies = pd.read_csv('clean_movies_comma.csv')  # First 20,000 movies
new_movies = pd.read_csv('clean_movies_metadataChanged.csv')  # New 40,000 movies

# Filter out duplicates
filtered_movies = new_movies[~new_movies['title'].isin(existing_movies['title'])]

# Save the filtered data to a new CSV (for importing into Neo4j)
filtered_movies.to_csv('filtered_movies.csv', index=False)

# Save the filtered data as a log of added movies
filtered_movies.to_csv('added_movies_log.csv', index=False)

print(f"Filtered {len(filtered_movies)} new movies and saved to 'filtered_movies.csv'.")
print(f"Log of added movies saved to 'added_movies_log.csv'.")