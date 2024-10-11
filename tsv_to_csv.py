import csv
import pandas as pd

def tsv_to_csv(input_tsv_file, output_csv_file):
    with open(input_tsv_file, 'r', newline='', encoding='utf-8') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for row in reader:
                writer.writerow(row)
    print(output_csv_file + ' created!')


# tsv_to_csv('name.basics.tsv/name.basics.tsv', 'name.basics.csv')
# tsv_to_csv('title.basics.tsv/title.basics.tsv', 'title.basics.csv')
# tsv_to_csv('title.principals.tsv/title.principals.tsv', 'title.principals.csv')
# tsv_to_csv('title.crew.tsv/title.crew.tsv', 'title.crew.csv')
# tsv_to_csv('title.ratings.tsv/title.ratings.tsv', 'title.ratings.csv')


def clean_title_basics(input_file, output_file, ratings_file):
    # Specify column data types for title.basics
    dtype_dict = {
        'tconst': 'string',
        'titleType': 'string',
        'primaryTitle': 'string',
        'startYear': 'string',  # Since it might contain '\N', treat as string
        'runtimeMinutes': 'string',  # Treat as string for missing values handling
        'genres': 'string',
        'isAdult': 'string'
    }

    # Load the title.basics CSV file into a DataFrame with specified dtypes
    df = pd.read_csv(input_file, delimiter=',', dtype=dtype_dict, low_memory=False)

    # Filter to keep only rows where the titleType is 'movie'
    df_filtered = df[df['titleType'] == 'movie']

    # Select only the desired columns from title.basics
    df_selected = df_filtered[['tconst', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres']]

    # Load the title.ratings CSV file into a DataFrame
    ratings_df = pd.read_csv(ratings_file, delimiter=',', dtype={
        'tconst': 'string',
        'averageRating': 'float64',
        'numVotes': 'Int64'  # Use 'Int64' (nullable integer type) to handle NaN values
    })

    # Merge the cleaned title.basics data with title.ratings on the 'tconst' column
    df_merged = pd.merge(df_selected, ratings_df, on='tconst', how='left')

    # Replace all instances of '\N' with None across the entire DataFrame
    df_merged.replace('\\N', None, inplace=True)

    # Save the merged data to a new CSV file
    df_merged.to_csv(output_file, index=False)
    #df_merged.to_csv(output_file, index=False, na_rep='None')

    print(input_file + ' cleaned!')
    

def clean_title_principals(input_file, output_file_full, output_file_part1, output_file_part2, output_file_part3, output_file_part4):
    # Load the title.principals file into a DataFrame
    df = pd.read_csv(input_file, delimiter=',', low_memory=False)

    # Specify any cleaning steps needed (e.g., filter, select columns, etc.)
    # Here you can add your cleaning logic as needed.
    # For example, let's assume you want to filter rows where tconst is not null
    df_cleaned = df[df['tconst'].notnull()]

    # Select only the desired columns (modify as needed)
    df_selected = df_cleaned[['tconst', 'nconst', 'category', 'job', 'characters']]

    # Save the full cleaned data to a new CSV file
    df_selected.to_csv(output_file_full, index=False)

    # Split the DataFrame into four parts
    num_rows = len(df_selected)
    part_size = num_rows // 4

    # Create parts; handle the last part to include any remaining rows
    df_part1 = df_selected.iloc[:part_size]
    df_part2 = df_selected.iloc[part_size:part_size * 2]
    df_part3 = df_selected.iloc[part_size * 2:part_size * 3]
    df_part4 = df_selected.iloc[part_size * 3:]

    # Save each part to separate CSV files
    df_part1.to_csv(output_file_part1, index=False)
    df_part2.to_csv(output_file_part2, index=False)
    df_part3.to_csv(output_file_part3, index=False)
    df_part4.to_csv(output_file_part4, index=False)
    
    print(input_file + ' cleaned!') 
    
    
def clean_name_basics(input_file, output_file_part1, output_file_part2, title_principals_file):
    # Load the cleaned title.principals file to get valid nconst values
    title_principals_df = pd.read_csv(title_principals_file, delimiter=',')
    valid_nconsts = set(title_principals_df['nconst'])

    # Specify column data types
    dtype_dict = {
        'nconst': 'string',
        'primaryName': 'string',
        'primaryProfession': 'string',
        'knownForTitles': 'string'
    }

    # Load the name.basics file into a DataFrame
    df = pd.read_csv(input_file, delimiter=',', dtype=dtype_dict, low_memory=False)

    # Filter by nconst values that are in the cleaned title.principals.csv file
    df_filtered = df[df['nconst'].isin(valid_nconsts)]

    # Select only the desired columns
    df_selected = df_filtered[['nconst', 'primaryName', 'primaryProfession', 'knownForTitles']]

    # Split the DataFrame into two parts
    midpoint = len(df_selected) // 2
    df_part1 = df_selected.iloc[:midpoint]  # First half
    df_part2 = df_selected.iloc[midpoint:]   # Second half

    # Save the cleaned data to two new CSV files
    df_part1.to_csv(output_file_part1, index=False)
    df_part2.to_csv(output_file_part2, index=False)
    
    print(input_file + ' cleaned!') 
    
def clean_title_crew(input_file, output_file, title_principals_file):
    # Load the cleaned title.principals file to get valid nconst values
    title_principals_df = pd.read_csv(title_principals_file, delimiter=',')
    valid_nconsts = set(title_principals_df['nconst'])

    # Specify column data types
    dtype_dict = {
        'tconst': 'string',
        'directors': 'string',
        'writers': 'string'
    }

    # Load the title.crew file into a DataFrame
    df = pd.read_csv(input_file, delimiter=',', dtype=dtype_dict, low_memory=False)

    # Filter by nconst values in directors and writers columns
    df_filtered = df[df['tconst'].isin(title_principals_df['tconst'])]

    # Select only the desired columns
    df_selected = df_filtered[['tconst', 'directors', 'writers']]

    # Save the cleaned data to a new CSV file
    df_selected.to_csv(output_file, index=False)   
    
    print(input_file + ' cleaned!') 
    
    

#clean_title_basics('title.basics.csv', 'title.basics_cleaned.csv', 'title.ratings.csv')
clean_title_principals('title.principals.csv', 'title.principals_cleaned.csv', 'title.principals_cleaned_part1.csv', 'title.principals_cleaned_part2.csv', 'title.principals_part3.csv', 'title.principals_part4.csv')
#clean_name_basics('name.basics.csv', 'name.basics_cleaned_part1.csv', 'name.basics_cleaned_part2.csv', 'title.principals_cleaned.csv')
#clean_title_crew('title.crew.csv', 'title.crew_cleaned.csv', 'title.principals_cleaned.csv')

