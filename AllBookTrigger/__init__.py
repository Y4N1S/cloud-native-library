import logging
import mysql.connector
import json
import os
import azure.functions as func

config = {
    'host' : os.environ['HOST_SQL_AZURE'],
    'user' : os.environ['USER_SQL_AZURE'],
    'password' : os.environ['PASSWORD_SQL_AZURE'],
    'database': os.environ['DATABASE_SQL_AZURE'],
    'client_flags' : [mysql.connector.ClientFlag.SSL],
    'ssl_ca': os.environ['SSL_CA_SQL_AZURE']}

conn = mysql.connector.connect(**config)
cursor = conn.cursor(dictionary=True)

    
def get_book(book):
    titre = (book,)
    cursor.execute(
        """
        SELECT *
        from epubs
        WHERE titre = %s""",
        titre
    )
    result = cursor.fetchone()
    return json.dumps(result)

def main(req: func.HttpRequest) -> func.HttpResponse:
    book = req.params.get('book')
    logging.info(f"{book}")
    return func.HttpResponse(
        get_book(book),
        mimetype="application/json",
        status_code=200
    )
    # return json.dumps(get_book(book))