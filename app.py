import requests
from bs4 import BeautifulSoup
import csv

# URL of the website
url = 'https://www.dawn.com'

def fetch_page(url):
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
    return None

def scrape_dawn_homepage(url):
    soup = fetch_page(url)
    
    if not soup:
        return []  # Return an empty list if the page failed to load

    articles_data = []  # Empty list to hold all articles data

    # Find all article tags with the specific class for stories and editorials
    articles = soup.find_all('article', class_='story')
    for article in articles:
        title_tag = article.find('h2', class_='story__title') or article.find('figure', class_='media')
        if title_tag:
            link_tag = title_tag.find('a')
            if link_tag and 'youtube' not in link_tag['href']:
                link = link_tag['href']
                title = link_tag.get('title') or link_tag.get_text(strip=True)
                excerpt = article.find('div', class_='story__excerpt').get_text(strip=True) if article.find('div', class_='story__excerpt') else ''
                
                # Fetching detailed content from the article's link
                detail_soup = fetch_page(link)
                if detail_soup:
                    content_div = detail_soup.find('div', class_='story__content')
                    if content_div:
                        content_paragraphs = content_div.find_all('p')
                        content = ' '.join(paragraph.get_text(strip=True) for paragraph in content_paragraphs)
                    else:
                        content = "No detailed content available."
                    articles_data.append([title, link, excerpt, content])
    
    return articles_data

# Scrape the data
data = scrape_dawn_homepage(url)

# Save the data to CSV
with open('dawn_articles.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Title', 'Link', 'Excerpt', 'Content'])
    writer.writerows(data)

print('Scraping completed and data stored in dawn_articles.csv')
