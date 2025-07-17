import requests
from bs4 import BeautifulSoup
import pyodbc
import json
import re
from datetime import datetime
import time
import logging
from urllib.parse import urljoin, urlparse
import hashlib
import sys

# Forțează codificarea UTF-8 pe Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

class NewsDatabase:
    def __init__(self, server='localhost', database='news_scraper', trusted_connection=True, username=None, password=None):
        self.server = server
        self.database = database
        self.trusted_connection = trusted_connection
        self.username = username
        self.password = password
        self.connection = None

    def connect(self):
        try:
            if self.trusted_connection:
                connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;'
            else:
                connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};'
            
            self.connection = pyodbc.connect(connection_string)
            logger.info("Conectat cu succes la baza de date SQL Server")
            return True
        except Exception as e:
            logger.error(f"Eroare la conectarea la baza de date: {e}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Conexiunea la baza de date a fost închisă")

    def article_exists(self, url):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.news WHERE url = ?", (url,))
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0
        except Exception as e:
            logger.error(f"Eroare la verificarea articolului: {e}")
            return False

    def insert_article(self, article_data):
        try:
            cursor = self.connection.cursor()
            insert_query = """
                INSERT INTO dbo.news (title, source, category, author, url, keywords, description, publishedAt, content, urlToImage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_query, (
                article_data['title'],
                article_data['source'],
                article_data['category'],
                article_data['author'],
                article_data['url'],
                article_data['keywords'],
                article_data['description'],
                article_data['publishedAt'],
                article_data['content'],
                article_data.get('urlToImage')
            ))
            self.connection.commit()
            cursor.close()
            logger.info(f"Articol insertat cu succes: {article_data['title'][:50]}...")
            return True
        except Exception as e:
            logger.error(f"Eroare la inserarea articolului: {e}")
            return False

class LLMDescriptionGenerator:
    def __init__(self, api_url="https://api.openai.com/v1/chat/completions", api_key="your_api_key"):
        self.api_url = api_url
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

    def generate_description(self, title, content, max_length=200):
        try:
            if not self.api_key or self.api_key == "your_api_key":
                return self.fallback_description(title, content, max_length)
            
            truncated_content = content[:1500] if content else ""
            
            prompt = f"""
            Creează o descriere scurtă și concisă în română pentru următoarea știre:
            
            Titlu: {title}
            Conținut: {truncated_content}
            
            Descrierea trebuie să fie între 50-{max_length} caractere și să rezume esențialul știrii.
            """
            
            payload = {
                "model": "gpt-3.5-turbo",
                "messages": [
                    {"role": "system", "content": "Ești un asistent care creează descrieri scurte pentru știri în limba română."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 100,
                "temperature": 0.7
            }
            
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                description = result['choices'][0]['message']['content'].strip()
                return description
            else:
                logger.warning(f"Eroare API LLM: {response.status_code}")
                return self.fallback_description(title, content, max_length)
                
        except Exception as e:
            logger.error(f"Eroare la generarea descrierii: {e}")
            return self.fallback_description(title, content, max_length)

    def fallback_description(self, title, content, max_length=200):
        if content:
            clean_content = re.sub(r'\s+', ' ', content.strip())
            sentences = re.split(r'[.!?]+', clean_content)
            
            description = ""
            for sentence in sentences:
                if len(description + sentence) < max_length - 10:
                    description += sentence.strip() + ". "
                else:
                    break
            
            return description.strip()
        else:
            return title[:max_length] if len(title) <= max_length else title[:max_length-3] + "..."

class NewsScraper:
    def __init__(self, db_config, llm_config=None):
        self.db = NewsDatabase(**db_config)
        self.llm_generator = LLMDescriptionGenerator(**llm_config) if llm_config else LLMDescriptionGenerator()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def extract_keywords(self, title, content):
        text = f"{title} {content}".lower()
        stop_words = {
            'și', 'sau', 'dar', 'care', 'pentru', 'din', 'cu', 'la', 'pe', 'de', 'în', 'să', 'că', 'nu',
            'se', 'au', 'este', 'sunt', 'era', 'vor', 'poate', 'toate', 'foarte', 'mai', 'după', 'până'
        }
        words = re.findall(r'\b[a-zA-ZăâîșțĂÂÎȘȚ]{3,}\b', text)
        keywords = [word for word in words if word not in stop_words]
        from collections import Counter
        word_freq = Counter(keywords)
        top_keywords = [word for word, count in word_freq.most_common(10)]
        return ', '.join(top_keywords)

    def scrape_hotnews(self):
        """Scrape articole de pe HotNews.ro"""
        logger.info("Începe scraping-ul pentru HotNews.ro")
        try:
            response = self.session.get('https://hotnews.ro', timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            article_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/stiri/' in href or '/articol/' in href:
                    full_url = urljoin('https://hotnews.ro', href)
                    if not full_url.startswith(('mailto:', 'https://www.facebook.com', 'https://twitter.com', 'https://whatsapp.com')):
                        article_links.append(full_url)
            
            article_links = list(set(article_links))[:20]
            
            logger.info(f"Găsite {len(article_links)} articole pe HotNews")
            
            for article_url in article_links:
                if self.db.article_exists(article_url):
                    logger.info(f"Articolul există deja: {article_url}")
                    continue
                
                article_data = self.scrape_single_article_hotnews(article_url)
                if article_data and article_data['title'] not in ["JavaScript is not available.", "Share on WhatsApp"]:
                    self.db.insert_article(article_data)
                
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Eroare la scraping HotNews: {e}")

    def scrape_single_article_hotnews(self, url):
        """Scrape un singur articol de pe HotNews"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            title_elem = soup.find('h1') or soup.find('title')
            title = title_elem.get_text(strip=True) if title_elem else "Titlu nedisponibil"
            if title in ["JavaScript is not available.", "Share on WhatsApp"] or not title:
                logger.warning(f"Titlu invalid pentru {url}: {title}")
                return None
            
            author_elem = soup.find('span', class_='author') or soup.find('div', class_='author')
            author = author_elem.get_text(strip=True) if author_elem else "Autor nedisponibil"
            
            content_elem = soup.find('div', class_='article-content') or soup.find('div', class_='content') or soup.find('article')
            content = ""
            if content_elem:
                for script in content_elem(["script", "style"]):
                    script.decompose()
                content = content_elem.get_text(strip=True)
            
            time_elem = soup.find('time') or soup.find('span', class_='date')
            published_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if time_elem and time_elem.get('datetime'):
                try:
                    published_at = datetime.fromisoformat(time_elem['datetime'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            description = self.llm_generator.generate_description(title, content)
            keywords = self.extract_keywords(title, content)
            
            image_elem = soup.find('meta', property='og:image')
            url_to_image = image_elem['content'] if image_elem else None
            
            category = "General"
            if any(word in title.lower() for word in ['politic', 'guvern', 'parlament']):
                category = "Politică"
            elif any(word in title.lower() for word in ['sport', 'fotbal', 'tenis']):
                category = "Sport"
            elif any(word in title.lower() for word in ['economic', 'bani', 'investiții']):
                category = "Economie"
            
            return {
                'title': title,
                'source': 'HotNews',
                'category': category,
                'author': author,
                'url': url,
                'keywords': keywords,
                'description': description,
                'publishedAt': published_at,
                'content': content[:1000],
                'urlToImage': url_to_image
            }
            
        except Exception as e:
            logger.error(f"Eroare la scraping articol {url}: {e}")
            return None

    def scrape_digi24(self):
        """Scrape articole de pe Digi24.ro"""
        logger.info("Începe scraping-ul pentru Digi24.ro")
        try:
            response = self.session.get('https://www.digi24.ro', timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            article_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/stiri/' in href:
                    full_url = urljoin('https://www.digi24.ro', href)
                    if not full_url.startswith(('mailto:', 'https://www.facebook.com', 'https://twitter.com', 'https://whatsapp.com')):
                        article_links.append(full_url)
            
            article_links = list(set(article_links))[:20]
            
            logger.info(f"Găsite {len(article_links)} articole pe Digi24")
            
            for article_url in article_links:
                if self.db.article_exists(article_url):
                    continue
                
                article_data = self.scrape_single_article_digi24(article_url)
                if article_data and article_data['title'] not in ["JavaScript is not available.", "Share on WhatsApp"]:
                    self.db.insert_article(article_data)
                
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Eroare la scraping Digi24: {e}")

    def scrape_single_article_digi24(self, url):
        """Scrape un singur articol de pe Digi24"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            title_elem = soup.find('h1')
            title = title_elem.get_text(strip=True) if title_elem else "Titlu nedisponibil"
            if title in ["JavaScript is not available.", "Share on WhatsApp"] or not title:
                logger.warning(f"Titlu invalid pentru {url}: {title}")
                return None
            
            author_elem = soup.find('span', class_='author')
            author = author_elem.get_text(strip=True) if author_elem else "Autor nedisponibil"
            
            content_elem = soup.find('div', class_='article-body')
            content = content_elem.get_text(strip=True) if content_elem else ""
            
            published_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            description = self.llm_generator.generate_description(title, content)
            keywords = self.extract_keywords(title, content)
            
            image_elem = soup.find('meta', property='og:image')
            url_to_image = image_elem['content'] if image_elem else None
            
            category = "General"
            if any(word in title.lower() for word in ['politic', 'guvern']):
                category = "Politică"
            elif any(word in title.lower() for word in ['sport']):
                category = "Sport"
            
            return {
                'title': title,
                'source': 'Digi24',
                'category': category,
                'author': author,
                'url': url,
                'keywords': keywords,
                'description': description,
                'publishedAt': published_at,
                'content': content[:1000],
                'urlToImage': url_to_image
            }
            
            
        except Exception as e:
            logger.error(f"Eroare la scraping articol Digi24 {url}: {e}")
            return None

    def run_scraping(self):
        """Rulează procesul complet de scraping"""
        if not self.db.connect():
            logger.error("Nu s-a putut conecta la baza de date")
            return
        try:
            logger.info("Începe procesul de scraping...")
            self.scrape_hotnews()
            self.scrape_digi24()
            logger.info("Procesul de scraping s-a terminat cu succes")
        except Exception as e:
            logger.error(f"Eroare generală în procesul de scraping: {e}")
        finally:
            self.db.disconnect()