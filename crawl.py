import requests
from bs4 import BeautifulSoup
import time

base_url = "https://www.baogiaothong.vn"
url = "https://www.baogiaothong.vn/chu-de/tai-nan-giao-thong-moi-nhat-trong-ngay-hom-nay-137.htm"

# Gửi request tới trang web
response = requests.get(url)
response.encoding = 'utf-8'

# Kiểm tra nếu request thành công
if response.status_code == 200:
    # Phân tích HTML của trang
    soup = BeautifulSoup(response.text, "html.parser")

    # Tìm tất cả các phần tử chứa link bài viết
    article_links = []
    articles = soup.find_all("div", class_="box-category-item")
    for article in articles:
        link_tag = article.find("a", class_="box-category-link-with-avatar")
        if link_tag:
            link = link_tag["href"]
            full_link = base_url + link
            article_links.append(full_link)
    filtered_urls = [url for url in article_links if "https://atgt.baogiaothong.vn/" not in url]

    # print(filtered_urls)
    for link in filtered_urls:
        time.sleep(1)

        response = requests.get(link)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            detail_cmain = soup.find("div", class_="detail-cmain")
            if detail_cmain:
                content = detail_cmain.get_text(strip=True)
                print(f"Nội dung bài báo tại {link}:\n{content}\n")
            else:
                print(f"Không tìm thấy nội dung bài báo tại {link}")
        else:
            print(f"Không thể truy cập bài báo tại {link}, mã trạng thái HTTP: {response.status_code}")

else:
    print(f"Không thể truy cập trang, mã trạng thái HTTP: {response.status_code}")
