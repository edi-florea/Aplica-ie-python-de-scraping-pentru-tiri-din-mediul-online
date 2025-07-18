import pyodbc
import logging

# Configurare logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_sql_server_connection():
    try:
        # Configurația pentru baza de date
        server = 'localhost\\SQLEXPRESS'
        database = 'news_scraper'
        
        # Pentru Windows Authentication
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        

        
        logger.info("Încearcă conectarea la SQL Server...")
        connection = pyodbc.connect(connection_string)
        logger.info("Conectare reușită!")
        
        # Testează o query simplă
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.news")
        count = cursor.fetchone()[0]
        logger.info(f"Numărul de articole în baza de date: {count}")
        
        # Testează structura tabelei
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'news' AND TABLE_SCHEMA = 'dbo'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        logger.info("Structura tabelei:")
        for col in columns:
            logger.info(f"  {col[0]} - {col[1]} - Null: {col[2]}")
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Eroare la conectare: {e}")
        return False

if __name__ == "__main__":
    test_sql_server_connection()