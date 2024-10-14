# CPTS415_Project

Movie Project for CptS 415

How to set up the database:

1. Clone this repository on your local device.
2. Download the title.basics, title.pricipals, title.ratings, and name.basic zip files from this link: https://datasets.imdbws.com/
3. Unzip all of the folders and place them in the same folder as the parser.py file.
4. Run the parser.py file
5. Create a loacl database using Neo4j Desktop
6. Import the following files into the database's import folder:
   - title.basics_cleaned.csv
   - name.basics_cleaned.csv
   - title.principals_cleaned_part1.csv
   - title.principals_cleaned_part2.csv
   - title.principals_cleaned_part3.csv
   - title.principals_cleaned_part4.csv
   - title.principals_cleaned_part5.csv
   - title.principals_cleaned_part6.csv
   - title.principals_cleaned_part7.csv
   - title.principals_cleaned_part8.csv
   - title.principals_cleaned_part9.csv
   - title.principals_cleaned_part10.csv
7. Run cypher commands in cypher_commands.txt to load nodes and edges into Neo4j.
8. Run test queries to ensure that the data is properly loaded.
