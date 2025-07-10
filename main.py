import os
import sys
import argparse
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv
import logging
import pyodbc

# Forțează codificarea UTF-8 pe Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# Încarcă variabilele de mediu
load_dotenv()

# Import modulele proprii
from news_scraper import NewsScraper
from api_server import app

# Configurare logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

def get_db_config():
    """Obține configurația bazei de date din variabilele de mediu"""
    return {
        'server': os.getenv('DB_HOST', 'localhost\\SQLEXPRESS'),
        'database': os.getenv('DB_NAME', 'news_scraper'),
        'username': os.getenv('DB_USER', 'your_username'),
        'password': os.getenv('DB_PASSWORD', 'your_password'),
        'trusted_connection': True
    }

def get_llm_config():
    """Obține configurația LLM din variabilele de mediu"""
    api_key = os.getenv('LLM_API_KEY')
    if api_key and api_key != 'your_api_key_here':
        return {
            'api_url': os.getenv('LLM_API_URL', 'https://api.openai.com/v1/chat/completions'),
            'api_key': api_key
        }
    return None

def run_scraper():
    """Rulează procesul de scraping"""
    try:
        logger.info("=== Începe procesul de scraping ===")
        db_config = get_db_config()
        llm_config = get_llm_config()
        scraper = NewsScraper(db_config, llm_config)
        scraper.run_scraping()
        logger.info("=== Procesul de scraping s-a terminat ===")
    except Exception as e:
        logger.error(f"Eroare în procesul de scraping: {e}")

def run_api_server():
    """Rulează serverul API"""
    try:
        logger.info("Pornește serverul API...")
        host = os.getenv('API_HOST', '0.0.0.0')
        port = int(os.getenv('API_PORT', 5000))
        debug = os.getenv('API_DEBUG', 'True').lower() == 'true'
        app.run(debug=debug, host=host, port=port)
    except Exception as e:
        logger.error(f"Eroare la pornirea serverului API: {e}")

def run_scheduler():
    """Rulează scheduler-ul pentru scraping automat"""
    logger.info("Pornește scheduler-ul pentru scraping automat...")
    schedule.every(30).minutes.do(run_scraper)
    run_scraper()
    while True:
        schedule.run_pending()
        time.sleep(60)

def test_connection():
    """Testează conexiunea la baza de date"""
    try:
        logger.info("Testează conexiunea la baza de date...")
        db_config = get_db_config()
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config['server']};DATABASE={db_config['database']};Trusted_Connection=yes;"
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.news")
        count = cursor.fetchone()[0]
        logger.info(f"Conexiune reușită! Numărul de articole în baza de date: {count}")
        cursor.close()
        connection.close()
    except Exception as e:
        logger.error(f"Eroare la testarea conexiunii: {e}")

def main():
    """Funcția principală"""
    parser = argparse.ArgumentParser(description='News Scraper Application')
    parser.add_argument('command', choices=['scrape', 'api', 'scheduler', 'test'], 
                        help='Comanda de executat')
    parser.add_argument('--sources', nargs='+', 
                        choices=['hotnews', 'digi24', 'all'], 
                        default=['all'],
                        help='Sursele pentru scraping')
    args = parser.parse_args()
    logger.info(f"Rulează comanda: {args.command}")
    if args.command == 'scrape':
        run_scraper()
    elif args.command == 'api':
        run_api_server()
    elif args.command == 'scheduler':
        run_scheduler()
    elif args.command == 'test':
        test_connection()
    else:
        print("Comandă nerecunoscută!")
        sys.exit(1)

if __name__ == "__main__":
    main()