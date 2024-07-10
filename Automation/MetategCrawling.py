import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import time
from collections import Counter
import re

def extract_keywords(text, top_n=5):
    words = re.findall(r'\w+', text.lower())
    return [word for word, _ in Counter(words).most_common(top_n) if len(word) > 2]

def crawl_seo_meta_tags(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        if response.encoding.lower() != 'utf-8':
            response.encoding = response.apparent_encoding
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        seo_info = {
            'URL': url,
            'Title': soup.title.string if soup.title else '',
            'Meta Description': '',
            'Meta Keywords': '',
            'Meta Robots': '',
            'Viewport': '',
            'Canonical URL': '',
            'og:title': '',
            'og:description': '',
            'og:image': '',
            'Hreflang': [],
            'Extracted Keywords': []
        }
        
        for tag in soup.find_all('meta'):
            if tag.get('name') == 'description':
                seo_info['Meta Description'] = tag.get('content', '')
            elif tag.get('name') == 'keywords':
                seo_info['Meta Keywords'] = tag.get('content', '')
            elif tag.get('name') == 'robots':
                seo_info['Meta Robots'] = tag.get('content', '')
            elif tag.get('name') == 'viewport':
                seo_info['Viewport'] = tag.get('content', '')
            elif tag.get('property') == 'og:title':
                seo_info['og:title'] = tag.get('content', '')
            elif tag.get('property') == 'og:description':
                seo_info['og:description'] = tag.get('content', '')
            elif tag.get('property') == 'og:image':
                seo_info['og:image'] = tag.get('content', '')
        
        canonical = soup.find('link', rel='canonical')
        if canonical:
            seo_info['Canonical URL'] = canonical.get('href', '')
        
        hreflangs = soup.find_all('link', rel='alternate', hreflang=True)
        seo_info['Hreflang'] = [f"{link['hreflang']}:{link['href']}" for link in hreflangs]
        
        title_keywords = extract_keywords(seo_info['Title'])
        desc_keywords = extract_keywords(seo_info['Meta Description'])
        seo_info['Extracted Keywords'] = list(set(title_keywords + desc_keywords))
        
        return seo_info
    except Exception as e:
        print(f"Error crawling {url}: {str(e)}")
        return {}

def get_internal_links(url, soup):
    internal_links = set()
    base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc == urlparse(base_url).netloc:
            internal_links.add(full_url)
    
    return internal_links

def crawl_site(start_url):
    visited = set()
    to_visit = {start_url}
    results = []

    while to_visit:
        url = to_visit.pop()
        if url not in visited:
            print(f"Crawling: {url}")
            visited.add(url)
            
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                if response.encoding.lower() != 'utf-8':
                    response.encoding = response.apparent_encoding
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                seo_info = crawl_seo_meta_tags(url)
                results.append(seo_info)
                
                internal_links = get_internal_links(url, soup)
                to_visit.update(internal_links - visited)
                
                time.sleep(2)  # 크롤링 간격을 2초로 증가
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")

    return results

def save_to_excel(data, filename='seo_meta_tags.xlsx'):
    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    print(f"Data saved to {filename}")

# 크롤링 시작
start_url = 'https://fastfive.co.kr/'
results = crawl_site(start_url)

# 엑셀로 저장
save_to_excel(results)
