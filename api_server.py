from flask import Flask, jsonify, request
import pyodbc
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

# Configurare logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Încarcă variabilele de mediu
load_dotenv()

app = Flask(__name__)

def get_db_connection():
    """Creează conexiunea la baza de date SQL Server"""
    try:
        server = os.getenv('DB_HOST', 'localhost\\SQLEXPRESS')
        database = os.getenv('DB_NAME', 'news_scraper')
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'Trusted_Connection=yes;'
        )
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        logger.error(f"Eroare la conectarea la baza de date: {e}")
        return None

@app.route('/api/news', methods=['GET'])
def get_news():
    """Returnează articole filtrate după parametri"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Nu s-a putut conecta la baza de date'}), 500
        
        cursor = conn.cursor()
        
        # Parametri de filtrare
        source = request.args.get('source')
        category = request.args.get('category')
        limit = request.args.get('limit', default=20, type=int)
        start_date = request.args.get('start_date')  # Filtru dată
        end_date = request.args.get('end_date')

        # Construirea query-ului dinamic
        query = """
            SELECT id, title, source, category, author, url, keywords, 
                   description, publishedAt, content, urlToImage 
            FROM dbo.news 
            WHERE 1=1
        """
        params = []
        
        if source:
            query += " AND source = ?"
            params.append(source)
        if category:
            query += " AND category = ?"
            params.append(category)
        if start_date:
            query += " AND publishedAt >= ?"
            params.append(start_date)
        if end_date:
            query += " AND publishedAt <= ?"
            params.append(end_date)
        
        query += " ORDER BY publishedAt DESC"
        if limit:
            query += f" OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        news = []
        for row in rows:
            news.append({
                'id': row.id,
                'title': row.title,
                'source': row.source,
                'category': row.category,
                'author': row.author,
                'url': row.url,
                'keywords': row.keywords,
                'description': row.description,
                'publishedAt': row.publishedAt.isoformat() if row.publishedAt else None,
                'content': row.content,
                'urlToImage': row.urlToImage
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(news)
    
    except Exception as e:
        logger.error(f"Eroare la obținerea articolelor: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/news/<int:id>', methods=['GET'])
def get_news_by_id(id):
    """Returnează un articol specific după ID"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Nu s-a putut conecta la baza de date'}), 500
        
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, title, source, category, author, url, keywords, 
                   description, publishedAt, content, urlToImage
            FROM dbo.news
            WHERE id = ?
            """,
            (id,)
        )
        row = cursor.fetchone()
        
        if not row:
            return jsonify({'error': 'Articolul nu a fost găsit'}), 404
        
        news_item = {
            'id': row.id,
            'title': row.title,
            'source': row.source,
            'category': row.category,
            'author': row.author,
            'url': row.url,
            'keywords': row.keywords,
            'description': row.description,
            'publishedAt': row.publishedAt.isoformat() if row.publishedAt else None,
            'content': row.content,
            'urlToImage': row.urlToImage
        }
        
        cursor.close()
        conn.close()
        
        return jsonify(news_item)
    
    except Exception as e:
        logger.error(f"Eroare la obținerea articolului cu ID {id}: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)