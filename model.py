import re
from datetime import datetime
from unidecode import unidecode

class Model:
    def __init__(self):
        pass

    def extract_information(self, text):
        # Xử lý số người chết
        num_deaths = 0
        death_match = re.search(r"(\d+)\s+người.*?tử vong", text)
        if death_match:
            num_deaths = int(death_match.group(1))
        else:
            # Trường hợp không có số, mặc định là 1 người tử vong nếu xuất hiện cụm "một người tử vong"
            if "một người" in text or "1 người" in text and "tử vong" in text:
                num_deaths = 1

        # Xử lý số người bị thương
        num_injured = 0
        injured_match = re.search(r"(\d+)\s+người.*?bị thương", text)
        if injured_match:
            num_injured = int(injured_match.group(1))
        else:
            # Trường hợp không có số, mặc định là 1 người bị thương
            if "một người" in text or "1 người" in text and "bị thương" in text:
                num_injured = 1

        # Xử lý địa điểm
        location = ""
        province_match = re.search(r"tỉnh\s+(\S+\s+\S+)", text)
        if province_match:
            location = province_match.group(1)
        else:
            location_match = re.search(r"tại\s+([\w\s]+)", text)
            if location_match:
                location = location_match.group(1).strip()

        # Loại bỏ dấu trong tên địa điểm
        if location:
            location = unidecode(location)

        # Xác định mức độ nghiêm trọng
        severity = "serious" if num_deaths > 0 else "light"

        return {
            "LocationAS": location,
            "IncidentType": "Va cham xe",
            "NumberOfInjured": num_injured,
            "NumberOfDeaths": num_deaths,
            "Serious": severity,
            "Weather": "Normal",
            "CreatedAt": f'{datetime.now().year}-{datetime.now().month:02}-{datetime.now().day:02}',
        }

# Ví dụ sử dụng
text_vi = ("Một vụ tai nạn giữa xe máy lại vừa xảy ra ở quốc lộ 12B, đoạn qua chợ Ngọc Mỹ, huyện Tân Lạc, tỉnh Hòa Bình.")
model = Model()
info = model.extract_information(text_vi)
print(info)
