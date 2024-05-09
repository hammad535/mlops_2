from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import csv

# URLs of the websites
dawn_url = 'https://www.dawn.com/'
bbc_url = 'https://www.bbc.com'

# Function to extract data from dawn.com
def scrape_dawn_homepage():
    response = requests.get(dawn_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    articles_data = []

    articles = soup.find_all('article', class_='story')
    for article in articles:
        title_tag = article.find('h2', class_='story__title') or article.find('figure', class_='media')
        if title_tag:
            link_tag = title_tag.find('a')
            if link_tag and 'youtube' not in link_tag['href']:
                link = link_tag['href']
                title = link_tag.get('title') or link_tag.get_text(strip=True)
                excerpt = article.find('div', class_='story__excerpt').get_text(strip=True) if article.find('div', class_='story__excerpt') else ''
                detail_response = requests.get(link)
                detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                content_div = detail_soup.find('div', class_='story__content')
                if content_div:
                    content_paragraphs = content_div.find_all('p')
                    content = ' '.join(paragraph.get_text(strip=True) for paragraph in content_paragraphs)
                else:
                    content = "No detailed content available."
                articles_data.append([title, link, excerpt, content])
    
    with open('/path/to/dawn_articles.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Link', 'Excerpt', 'Content'])
        writer.writerows(articles_data)

# Function to extract data from bbc.com
def scrape_bbc_homepage():
    response = requests.get(bbc_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    articles_data = []

    articles = soup.find_all('div', attrs={"data-testid": "dundee-card"})
    articles += soup.find_all('div', attrs={"data-testid": "manchester-card"})  # Adding other patterns if needed

    for article in articles:
        link_tag = article.find('a', {'data-testid': 'internal-link'})
        if link_tag:
            link = bbc_url + link_tag['href']
            title = article.find('h2').get_text(strip=True) if article.find('h2') else 'No headline available'
            description = article.find('p', {'data-testid': 'card-description'}).get_text(strip=True) if article.find('p', {'data-testid': 'card-description'}) else 'No description available'
            detail_response = requests.get(link)
            detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
            content_blocks = detail_soup.find_all('div', attrs={"data-component": "text-block"})
            content = ' '.join(p.get_text(strip=True) for block in content_blocks for p in block.find_all('p', class_='sc-e1853509-0 bmLndb'))
            if not content:
                content = "No detailed content available."
            articles_data.append([title, link, description, content])

    with open('/path/to/bbc_articles.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Headline', 'Link', 'Description', 'Content'])
        writer.writerows(articles_data)

# Initialize the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 8),
    'retries': 1,
}

dag = DAG(
    'news_extraction_dag',
    default_args=default_args,
    description='A DAG to extract news articles from dawn.com and bbc.com',
    schedule_interval='@daily',
    catchup=False
)

# Define tasks
extract_dawn_task = PythonOperator(
    task_id='extract_dawn_articles',
    python_callable=scrape_dawn_homepage,
    dag=dag
)

extract_bbc_task = PythonOperator(
    task_id='extract_bbc_articles',
    python_callable=scrape_bbc_homepage,
    dag=dag
)

# Define task dependencies
extract_dawn_task >> extract_bbc_task
