import csv
import os
import psycopg2
import pandas as pd

# Database Connection
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="Mushroom",
    user="postgres",
    password="2414"
)

cur = conn.cursor()
create_table_query = '''
CREATE TABLE Mushroom3(
    Class TEXT,
    Habitat TEXT,
    IMAGE Bytea
)
'''
cur.execute(create_table_query)
conn.commit()

# Set the directory path
directory = "./Images"
# Get a list of file names in the directory
file_names = os.listdir(directory)

# Create an empty array to store the image data
images = []

# Loop over the file names
for file_name in file_names:
    # Check if the file is an image
    if file_name.endswith('.jpg') or file_name.endswith('.png'):
        # Construct the full path to the image file
        image_path = os.path.join(directory, file_name)
        # Add the image data to the array
        images.append({'name': file_name, 'path': image_path})        
        
# Reading the CSV File.
with open('New_Dataset.csv', newline='') as csvfile:
    # Create a CSV reader object
    reader = csv.reader(csvfile)
    
    # Skip the header row
    next(reader)
    
    # Loop over each row in the CSV file   
    for x in reader:
        for image in images:
            if image['name'][0] == x[0]: #index for class
                if image['name'][1]==x[1]: #index for habitat
                    with open(image['path'],"rb") as image_file:
                        encoded_image = psycopg2.Binary(image_file.read())
                        cur.execute("""
                            INSERT INTO Mushroom3(Class,Habitat,IMAGE)
                            VALUES (%s, %s, %s);
                        """, (x[0],x[1], encoded_image))
                    conn.commit()

print("Dataset Created Successfully")

# To see the Image from SQL for class= Edible and habitat=  Wood Mushroom.
import psycopg2
from PIL import Image
import io
import matplotlib.pyplot as plt

# cur.execute("SELECT image FROM mushroom3 WHERE class = %s AND habitat=%s", ('e','d'))
# row = cur.fetchone()

# # Convert the binary string into an image object
# image_binary = row[0]
# image = Image.open(io.BytesIO(image_binary))

# # Display the image
# plt.imshow(image)
# plt.show()

print("Done with 1st stage!!!")

# Opening the SQL dataset in python
def sql_to_dataframe(conn, query, column_names):
    """ 
    Import data from a PostgreSQL database using a SELECT query 
    """
    cur = conn.cursor()
    print("Inside function")
    try:
        cur.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
        return 1
    # The execute returns a list of tuples:
    tuples_list = cur.fetchall()
    # Now we need to transform the list into a pandas DataFrame:
    df = pd.DataFrame(tuples_list, columns=column_names)
    return df

#creating a query variable to store our query to pass into the function
query = """SELECT Class, Habitat, Image FROM mushroom3"""
#creating a list with columns names to pass into the function
column_names = ['Class','Habitat', 'Image']

print("Done!!!")
# Calling the Fucntion
df = sql_to_dataframe(conn, query, column_names)

# Let’s see if we loaded the df successfully or not?
print(df.head())

Mushroom_data_df = pd.read_csv("agaricus-lepiota (3).data")
Mushroom_data_df = Mushroom_data_df[['Class','Bruises',
                    'Gill-Size','Gill-Spacing','Gill-Color',
                    'Habitat','Population','Stalk-Suraface-above-ring',
                    'Stalk-Suraface-below-ring']]
print(Mushroom_data_df.head())

merged_df = pd.merge(df, Mushroom_data_df, on=['Class', 'Habitat'], how='left')
merged_df.to_csv('Merge.csv')
print(merged_df)

def imageShow(index, merged_df):
    print(merged_df.iloc[index])
    image = Image.open(io.BytesIO(merged_df.iloc[index]['Image']))
    plt.imshow(image)
    plt.show()

imageShow(5040,merged_df)





