import requests
from bs4 import BeautifulSoup
import time
import csv
from model import Model
from datetime import datetime
from connect import  Connect
# 1.connect db
connection = None
sourceFile = []
locationFile = []
model = Model()
# Hàm kết nối cơ sở dữ liệu
async def connect():
    global connection
    connect = Connect()
    connection = await connect.connectODBC()

# Hàm chính để truy cập bảng ConfigFile
async def access_config_table():
    global sourceFile, locationFile
    if connection:
        cursor = connection.cursor()
        cursor.execute('SELECT SourceFile, LocationFile FROM ConfigFile')
        result = cursor.fetchall()
        print("Query Result:", result)
        try:
            # Gán dữ liệu vào danh sách
            sourceFile = [row[0] for row in result]
            locationFile = [row[1] for row in result]
            print("SourceFile:", sourceFile)
            print("LocationFile:", locationFile)
        except Exception as error:
            print(f"Error processing query result: {error}")
        finally:
            connection.close()  # Đóng kết nối sau khi sử dụng
# hàm cào dữ liệu
def craw_data(sources, locations):
    for i in range(len(sources)):
        source = sources[i]
        location = locations[i]
        base_url = "https://www.baogiaothong.vn"

        # Bước 1: Xóa dữ liệu cũ và ghi header trước khi cào
        try:
            with open(location, "w", newline="", encoding="utf-8") as file:
                # Tạo header từ model.extract_information, không có 'Id'
                sample_dict = model.extract_information("")
                fieldnames = ["Id"] + list(sample_dict.keys())  # Đưa Id lên đầu
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()  # Ghi dòng tiêu đề
                print(f"Đã xóa dữ liệu cũ và tạo header cho file: {location}")
        except Exception as error:
            print(f"Lỗi khi xóa dữ liệu cũ: {error}")
            continue

        # Cào dữ liệu
        response = requests.get(source)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            article_links = []
            articles = soup.find_all("div", class_="box-category-item")
            for article in articles:
                link_tag = article.find("a", class_="box-category-link-with-avatar")
                if link_tag:
                    link = link_tag["href"]
                    full_link = base_url + link
                    article_links.append(full_link)
            filtered_urls = [url for url in article_links if "https://atgt.baogiaothong.vn/" not in url]

            # Duyệt qua các liên kết bài viết và ghi dữ liệu
            for index, link in enumerate(filtered_urls, start=1):  # Dùng index cho Id
                time.sleep(1)

                response = requests.get(link)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    detail_cmain = soup.find("div", class_="detail__cnt-top")
                    if detail_cmain:
                        content = detail_cmain.get_text(strip=True)
                        dict = model.extract_information(content)
                        dict["CreatedAt"] = datetime.now().strftime("%Y-%m-%d")
                        dict["Id"] = index  # Gán Id cho mỗi bài viết

                        file_path = locationFile[i]
                        try:
                            with open(file_path, mode='a', newline='', encoding='utf-8') as file:
                                writer = csv.DictWriter(file, fieldnames=fieldnames)
                                writer.writerow(dict)  # Ghi dữ liệu vào file
                                print(f"Write oke for Id {index}")
                        except Exception as error:
                            print(f"Error writing: {error}")
                    else:
                        print(f"Không tìm thấy nội dung bài báo tại {link}")

                else:
                    print(f"Không thể truy cập bài báo tại {link}, mã trạng thái HTTP: {response.status_code}")
        else:
            print(f"Không thể truy cập trang, mã trạng thái HTTP: {response.status_code}")


# Hàm bất đồng bộ chính
async def main():
    await connect()  # Kết nối cơ sở dữ liệu
    print("Connection Type:", type(connection))  # Kiểm tra loại của connection
    await access_config_table()  # Truy cập bảng ConfigFile
    craw_data(sourceFile, locationFile)

# Chạy hàm chính
import asyncio
asyncio.run(main())

