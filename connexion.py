import mysql.connector
from mysql.connector import errorcode
import re

def deltable():
  # Drop previous table of same name if one exists
  cursor.execute("DROP TABLE IF EXISTS epubs;")
  print("Finished dropping table (if existed).")

def create_table():
  # Create table
  cursor.execute("CREATE TABLE epubs (id serial PRIMARY KEY, titre VARCHAR(50), url VARCHAR(250));")
  print("Finished creating table.")

def insert_row(data):
    # Insert some data into table
    cursor.execute("INSERT INTO epubs (titre, url) VALUES (%s, %s);", data)
    print("Inserted",cursor.rowcount,"row(s) of data.")
    # cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("orange", 154))
    # print("Inserted",cursor.rowcount,"row(s) of data.")
    # cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("apple", 100))
    # print("Inserted",cursor.rowcount,"row(s) of data.")


def see_all():
  cursor.execute("SELECT * FROM epubs;")
  result = cursor.fetchall()
  # print(result)


# Obtain connection string information from the portal
# config = {
#   'host':'equipe3.mysql.database.azure.com',
#   'user':'equipe3@equipe3',
#   'password':'Perrier!',
#   'database':'<mydatabase>',
#   'client_flags': [mysql.connector.ClientFlag.SSL],
#   'ssl_ca': '/var/wwww/html/DigiCertGlobalRootG2.crt.pem'
# }

# Construct connection string
try:
   conn = mysql.connector.connect(user="equipe3@equipe3", password="Perrier!", host="equipe3.mysql.database.azure.com", port=3306, database="ebooks", ssl_ca="DigiCertGlobalRootG2.crt.pem", ssl_verify_cert=True) 
#    conn = mysql.connector.connect(**config)
   print("Connection established")
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with the user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  cursor = conn.cursor()

  # deltable()
  # create_table()
  # insert_row(("juljan", "http://juljan.com"))
  see_all()

  # Cleanup
  conn.commit()
  cursor.close()
  conn.close()
  print("Done.")