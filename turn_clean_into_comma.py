import re

def remove_quotes_and_parentheses():
    with open("clean_movies.csv", 'r', encoding='utf-8') as infile, \
         open("clean_movies_comma.csv", 'w', encoding='utf-8') as outfile:
        for line in infile:
            # Remove quotes
            line = line.replace('"', '')
            # Remove content inside parentheses (including parentheses)
            line = re.sub(r'\(.*?\)', '', line)
            # Strip leading/trailing whitespace and add a comma
            cleaned_line = line.strip() + ',\n'
            outfile.write(cleaned_line)

# Usage
remove_quotes_and_parentheses()
