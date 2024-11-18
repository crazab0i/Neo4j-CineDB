import csv

def extract_titles_from_movie_csv():
    with open("movies.csv", mode="r", newline="", encoding="utf-8") as csvread, \
         open("clean_movies.csv", mode="w", newline="", encoding="utf-8") as csvwrite:
        reader = csv.DictReader(csvread)
        writer = csv.writer(csvwrite)

        writer.writerow(['title'])

        row_count = 0
        for row in reader:
            if row_count % 50 == 0:
                print(f"Finished with {row_count % 50 * 50} rows")
            writer.writerow([row['title']])
            row_count += 1

extract_titles_from_movie_csv()