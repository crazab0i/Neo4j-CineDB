import csv

def process_csv(input_file, output_file):
    # Combined Read and Write Process
    cleaned_data = []
    
    try:
        # Step 1: Read the input CSV
        print("Reading input CSV...")
        with open(input_file, "r", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            for row in reader:
                # Clean each row (remove leading/trailing whitespace and commas)
                cleaned_row = [item.strip().strip(",") for item in row if item]
                if cleaned_row:  # Skip empty rows
                    cleaned_data.append(cleaned_row)
        
        print(f"Successfully read {len(cleaned_data)} rows from {input_file}.\n")
        
        # Step 2: Write the cleaned data to a new CSV
        print("Writing cleaned data to output CSV...")
        with open(output_file, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile)
            writer.writerows(cleaned_data)
        
        print(f"Successfully wrote cleaned data to {output_file}.\n")
    
    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
input_csv_file = "clean_movies_metadataChanged.csv"
output_csv_file = "cleaner_movies_metadataChanged.csv"

process_csv(input_csv_file, output_csv_file)