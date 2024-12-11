import pandas as pd
import asyncio
from connect import Connect
from writeLog import writeLog


# Hàm xử lý và chuẩn hóa dữ liệu
async def transform(data):
    try:
        # 1. Tạo cột `date_time` từ `date` và `time`
        data['date_time'] = pd.to_datetime(
            data['date'].astype(str) + ' ' + data['time'].astype(str),
            errors='coerce'
        )

        # 2. Xử lý dữ liệu thiếu hoặc không hợp lệ
        # Loại bỏ các dòng thiếu thông tin chính
        data = data.dropna(subset=['date_time', 'location', 'type']).copy()

        # Xử lý cột 'damage': chuyển về số nguyên, thay giá trị lỗi bằng 0
        data.loc[:, 'damage'] = pd.to_numeric(data['damage'], errors='coerce').fillna(0).astype(int)

        # 3. Chuẩn hóa nội dung văn bản
        data.loc[:, 'location'] = data['location'].str.strip().str.title()
        data.loc[:, 'type'] = data['type'].str.strip().str.lower()

        # 4. Chọn các cột cần thiết
        data_cleaned = data[['date_time', 'location', 'type', 'damage']]

        print("Dữ liệu đã được chuẩn hóa thành công!")
        return data_cleaned

    except Exception as e:
        print(f"Lỗi trong quá trình Transform: {e}")
        return None


# Hàm main để kết nối và xử lý dữ liệu
async def main():
    # 1. Kết nối cơ sở dữ liệu
    connect = Connect()

    # 2. Lấy dữ liệu từ bảng staging từ cơ sở dữ liệu
    connection = await connect.connectODBC()
    if connection:
        cursor = connection.cursor()
        cursor.execute('SELECT Id, LocationFile FROM ConfigFile')  # Giả sử bảng ConfigFile chứa thông tin file
        result = cursor.fetchall()
        location_files = [row[1] for row in result]
        Id = [row[0] for row in result]

        # 3. Xử lý từng file
        try:
            for location in location_files:
                # Đọc file từ vị trí
                data = pd.read_csv(location)  # Giả sử đây là dạng file CSV

                # Chạy hàm transform để chuẩn hóa dữ liệu
                cleaned_data = await transform(data)

                if cleaned_data is not None:
                    # 4. Chèn dữ liệu vào bảng staging (hoặc bảng đích) sau khi xử lý
                    # Lưu vào cơ sở dữ liệu
                    cleaned_data.to_sql('staging_table', con=connection, if_exists='append', index=False)
                    await writeLog(Id[0], "ER", f"Load staging from {location} successfully")
                else:
                    await writeLog(Id[0], "ERR", f"Failed to transform data from {location}")

        except Exception as error:
            await writeLog(Id[0], "ERR", f"Error processing files: {str(error)}")

        finally:
            connection.close()  # Đảm bảo đóng kết nối khi xong


# Chạy hàm async
if __name__ == '__main__':
    asyncio.run(main())