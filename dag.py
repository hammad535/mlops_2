from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import csv
import subprocess
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['example@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'article_extraction_pipeline',
    default_args=default_args,
    description='mlops a2 etl pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)

def fetch_page(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def scrape_bbc_articles(url):
    bbc_url = "https://www.bbc.com"
    soup = fetch_page(url)
    
    if not soup:
        return []

    articles_data = []
    articles = soup.find_all('div', attrs={"data-testid": "dundee-card"})
    articles += soup.find_all('div', attrs={"data-testid": "manchester-card"})

    for article in articles:
        link_tag = article.find('a', {'data-testid': 'internal-link'})
        if link_tag:
            link = bbc_url + link_tag['href']
            title = article.find('h2').get_text(strip=True) if article.find('h2') else 'No headline available'
            description = article.find('p', {'data-testid': 'card-description'}).get_text(strip=True) if article.find('p', {'data-testid': 'card-description'}) else 'No description available'
            
            articles_data.append([title, link, description])
    
    return articles_data

    

def scrape_dawn_articles(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    articles = soup.find_all('article')
    data = []
    for article in articles:
        st = article.find(class_="story__title")
        story_excerpt = article.find(class_='story__excerpt')
        if st:
            title = st.text.strip()
            link = article.find(class_="story__link")['href']
            description = ''
            if story_excerpt:
                description = story_excerpt.text.strip()
            data.append([title, link, description])
    return data

def extract_articles():
    dawn_url = 'https://www.dawn.com/'
    bbc_url = 'https://www.bbc.com/'
    dawn_data = scrape_dawn_articles(dawn_url)
    bbc_data = scrape_bbc_articles(bbc_url)

    return dawn_data + bbc_data

def clean_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_articles')
    cleaned_data = [row for row in extracted_data if row[2]]  
    return cleaned_data

def save_to_csv(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='clean_data')
    filename = '/home/umer/Downloads/mlops a1/articles.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Title', 'Link', 'Description'])
        writer.writerows(data)
    return filename

def push_to_dvc(**kwargs):
    filename = '/home/umer/Downloads/mlops a1/articles.csv' 
    #subprocess.run(['dvc', 'init'], check=True)
    subprocess.run(['dvc', 'add', filename], check=True)
    subprocess.run(['dvc', 'push'], check=True)
    subprocess.run(['git', 'add', f'{filename}.dvc'], check=True)
    subprocess.run(['git', 'commit', '-m', 'Added data file to DVC'], check=True)
    subprocess.run(['git', 'push', '-u', 'origin', 'main'], check=True)

# Task definitions
extract_task = PythonOperator(
    task_id='extract_articles',
    python_callable=extract_articles,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_to_csv,
    provide_context=True,
    dag=dag,
)

push_task = PythonOperator(
    task_id='push_to_dvc',
    python_callable=push_to_dvc,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> clean_task >> save_task >> push_task
