import requests
from bs4 import BeautifulSoup
import time
import csv

# URL of the website
bbc_url = 'https://www.bbc.com'


def fetch_page(url, debug=False):
    try:
        response = requests.get(url, timeout=10)  # Increased timeout for better reliability
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            p_tags = soup.find_all('p')  # Find all paragraph tags
            for p in p_tags:
                print(p.get_text())  # Print the text content of each paragraph tag
            if debug:
                print(soup.prettify())  # Print the prettified HTML if debug is enabled
            return soup
        else:
            print(f"Failed to fetch {url}: Status code {response.status_code}")
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
    return None

 
fetch_page(bbc_url)


def scrape_bbc_homepage(url):
    soup = fetch_page(url)
    
    if not soup:
        return []  # Return an empty list if the page failed to load

    articles_data = []  # Empty list to hold all articles data

    # Locate the articles by their data-testid attributes
    articles = soup.find_all('div', attrs={"data-testid": "dundee-card"})
    articles += soup.find_all('div', attrs={"data-testid": "manchester-card"})  # Adding other patterns if needed

    for article in articles:
        link_tag = article.find('a', {'data-testid': 'internal-link'})
        if link_tag:
            link = bbc_url + link_tag['href']
            title = article.find('h2').get_text(strip=True) if article.find('h2') else 'No headline available'
            description = article.find('p', {'data-testid': 'card-description'}).get_text(strip=True) if article.find('p', {'data-testid': 'card-description'}) else 'No description available'
            
            # Fetching detailed content from the article's link with debug enabled
            detail_soup = fetch_page(link, debug=True)  # Enable debug to print HTML
            if detail_soup:
                content_blocks = detail_soup.find_all('div', attrs={"data-component": "text-block"})
                content = ' '.join(p.get_text(strip=True) for block in content_blocks for p in block.find_all('p', class_='sc-e1853509-0 bmLndb'))
                if not content:
                    print(f"No content found in detailed page: {link}")
            else:
                content = "No detailed content available."

            articles_data.append([title, link, description, content])
            print('\ntitle: '+ title + "\nContent: "+content)
    
    return articles_data

# Scrape the data
bbc_data = scrape_bbc_homepage(bbc_url)
# Save the data to CSV
with open('bbc_articles.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Headline', 'Link', 'Description', 'Content'])
    writer.writerows(bbc_data)

print('Scraping completed and data stored in bbc_articles.csv')
