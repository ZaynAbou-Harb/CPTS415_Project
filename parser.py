import csv
import pandas as pd

# IMPORTANT: Before you use the parser, make sure to download the title.basic, title.principals, title.ratings, name.basics data tables.
# Use this link: https://datasets.imdbws.com/
# Then unzip each folder and put the unziped folders in the same folder as parser.

# Converts the tsv files to csv files for processing
def tsv_to_csv(input_tsv_file, output_csv_file):
    with open(input_tsv_file, 'r', newline='', encoding='utf-8') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for row in reader:
                writer.writerow(row)
    print(output_csv_file + ' created!')

# Cleans and processes title_basics
def clean_title_basics(input_file, output_file, ratings_file):
    dtype_dict = {
        'tconst': 'string',
        'titleType': 'string',
        'primaryTitle': 'string',
        'startYear': 'string', 
        'runtimeMinutes': 'string',
        'genres': 'string',
        'isAdult': 'string'
    }

    df = pd.read_csv(input_file, delimiter=',', dtype=dtype_dict, low_memory=False)

    # Filter to keep only rows where the titleType is 'movie'
    df_filtered = df[df['titleType'] == 'movie']
    #df_filtered = df_filtered[df_filtered['startYear'] > '1979']
    
    # Select only the desired columns from title.basics
    df_selected = df_filtered[['tconst', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres']]

    ratings_df = pd.read_csv(ratings_file, delimiter=',', dtype={
        'tconst': 'string',
        'averageRating': 'float64',
        'numVotes': 'Int64'
    })

    # Merge the cleaned title.basics data with title.ratings on the 'tconst' column
    df_merged = pd.merge(df_selected, ratings_df, on='tconst', how='left')
    df_merged = df_merged[df_merged['numVotes'] >= 1000]

    # Replace all instances of '\N' with None across the entire DataFrame
    df_merged.replace('\\N', None, inplace=True)

    # Save the merged data to a new CSV file
    df_merged.to_csv(output_file, index=False)
    #df_merged.to_csv(output_file, index=False, na_rep='None')

    print(input_file + ' cleaned!')

# Cleans and processes title_principals  
def clean_title_principals(input_file, title_basics_file, output_file_full, *output_files):
    df_principals = pd.read_csv(input_file, delimiter=',', low_memory=False)
    
    # Clean the file of '\"' before processing
    df_principals['tconst'] = df_principals['tconst'].str.replace('\"', '', regex=False)
    df_principals['nconst'] = df_principals['nconst'].str.replace('\"', '', regex=False)
    df_principals['category'] = df_principals['category'].str.replace('\"', '', regex=False)
    df_principals['job'] = df_principals['job'].str.replace('\"', '', regex=False)
    df_principals['characters'] = df_principals['characters'].str.replace('\"', '', regex=False)
    print("removed substring")
    
    df_basics = pd.read_csv(title_basics_file, delimiter=',', low_memory=False)
    # Get the valid tconst from title.basic
    valid_tconsts = set(df_basics['tconst'])

    # Filter the title.principals rows where tconst is in the valid_tconsts set
    df_cleaned = df_principals[df_principals['tconst'].isin(valid_tconsts)]
    print("Sorted tconst")
    df_cleaned = df_cleaned[df_cleaned['category'].isin(['actor','actress','director'])]
    print("cleaned non actor etc")

    # Select only the desired columns
    df_selected = df_cleaned[['tconst', 'nconst', 'category', 'characters']]

    df_selected.to_csv(output_file_full, index=False)

    # Split the DataFrame into the desired number of parts (10 in this case)
    num_rows = len(df_selected)
    num_parts = 10
    part_size = num_rows // num_parts

    # Create parts; handle the last part to include any remaining rows
    for i in range(num_parts):
        start_index = i * part_size
        if i == num_parts - 1:
            df_part = df_selected.iloc[start_index:]
        else:
            df_part = df_selected.iloc[start_index:start_index + part_size]

        # Save each part to the corresponding CSV file
        df_part.to_csv(output_files[i], index=False)
    
    print(input_file + ' cleaned and split!') 

# Cleans and processes name_basics
def clean_name_basics(input_file, output_file, title_principals_file):
    title_principals_df = pd.read_csv(title_principals_file, delimiter=',')
    valid_nconsts = set(title_principals_df['nconst'])

    dtype_dict = {
        'nconst': 'string',
        'primaryName': 'string',
        'primaryProfession': 'string',
        'knownForTitles': 'string'
    }

    df = pd.read_csv(input_file, delimiter=',', dtype=dtype_dict, low_memory=False)

    # Filter by nconst values that are in the cleaned title.principals.csv file
    df_filtered = df[df['nconst'].isin(valid_nconsts)]

    # Select only the desired columns
    df_selected = df_filtered[['nconst', 'primaryName', 'primaryProfession', 'knownForTitles']]

    df_selected.to_csv(output_file, index=False)
    
    print(input_file + ' cleaned!') 

# Runs all of the functions
def execute ():
    # tsv to csv
    tsv_to_csv('name.basics.tsv/name.basics.tsv', 'name.basics.csv')
    tsv_to_csv('title.basics.tsv/title.basics.tsv', 'title.basics.csv')
    tsv_to_csv('title.principals.tsv/title.principals.tsv', 'title.principals.csv')
    tsv_to_csv('title.ratings.tsv/title.ratings.tsv', 'title.ratings.csv')   
    
    # Data cleaning and processing
    clean_title_basics('title.basics.csv', 'title.basics_cleaned.csv', 'title.ratings.csv')
    clean_title_principals(
        'title.principals.csv',
        'title.basics_cleaned.csv',
        'title.principals_cleaned.csv',
        'title.principals_cleaned_part1.csv',
        'title.principals_cleaned_part2.csv',
        'title.principals_cleaned_part3.csv',
        'title.principals_cleaned_part4.csv',
        'title.principals_cleaned_part5.csv',
        'title.principals_cleaned_part6.csv',
        'title.principals_cleaned_part7.csv',
        'title.principals_cleaned_part8.csv',
        'title.principals_cleaned_part9.csv',
        'title.principals_cleaned_part10.csv'
    )
    clean_name_basics('name.basics.csv', 'name.basics_cleaned.csv', 'title.principals_cleaned.csv')

# Execute all funtions
execute()