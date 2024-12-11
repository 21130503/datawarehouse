import tkinter as tk
from tkinter import ttk
import asyncio
from connect import Connect

data = []  # Dữ liệu toàn cục để hiển thị trong bảng

async def fetch_data():
    global data
    connect = Connect()
    connection = await connect.connectODBC()
    if connection:
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM AccidentFact')
        result = cursor.fetchall()
        data = [
            {
                "ID": row[0],
                "Địa điểm": row[1],
                "Loại tai nạn": row[2],
                "Số người bị thương": row[3],
                "Số người chết": row[4],
                "Mức độ nghiêm trọng": row[5],
                "Thời tiết": row[6],
                "Ngày" : row[7]
            }
            for row in result
        ]
        connection.close()
        update_table()

def update_table():
    for row in table.get_children():
        table.delete(row)
    for item in data:
        table.insert("", "end", values=(
            item["ID"], item["Ngày"], item["Địa điểm"],
            item["Số người chết"], item["Số người bị thương"],
            item["Mức độ nghiêm trọng"], item["Loại tai nạn"], item['Loại tai nạn']
        ))

def filter_data(*args):
    search_term = search_var.get().lower()
    for row in table.get_children():
        table.delete(row)
    for item in data:
        if search_term in str(item.values()).lower():
            table.insert("", "end", values=(
                item["ID"], item["Ngày"], item["Địa điểm"],
                item["Số người chết"], item["Số người bị thương"],
                item["Mức độ nghiêm trọng"], item["Loại tai nạn"],item['Loại tai nạn']
            ))

# Tạo cửa sổ chính
root = tk.Tk()
root.title("Dữ Liệu Tai Nạn Giao Thông")

# Tiêu đề
header_label = tk.Label(root, text="Dữ Liệu Tai Nạn Giao Thông", font=("Arial", 16))
header_label.pack(pady=10)

# Trường tìm kiếm
search_var = tk.StringVar()
search_var.trace_add("write", filter_data)
search_entry = tk.Entry(root, textvariable=search_var, font=("Arial", 12), width=30)
search_entry.pack(pady=5)
search_entry.insert(0, "Tìm kiếm...")

# Bảng hiển thị dữ liệu
table = ttk.Treeview(root, columns=("ID", "Ngày", "Địa điểm", "Số người chết", "Số người bị thương", "Mức độ nghiêm trọng", "Loại tai nạn"), show="headings")
table.pack(pady=10, fill=tk.BOTH, expand=True)

# Định nghĩa tiêu đề các cột
columns = ["ID", "Ngày", "Địa điểm", "Số người chết", "Số người bị thương", "Mức độ nghiêm trọng", "Loại tai nạn"]
for col in columns:
    table.heading(col, text=col)
    table.column(col, anchor="center")

# Thanh cuộn
x_scrollbar = ttk.Scrollbar(root, orient=tk.HORIZONTAL, command=table.xview)
y_scrollbar = ttk.Scrollbar(root, orient=tk.VERTICAL, command=table.yview)
table.configure(xscrollcommand=x_scrollbar.set, yscrollcommand=y_scrollbar.set)
x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

# Chạy hàm bất đồng bộ để tải dữ liệu
async def main():
    await fetch_data()
    root.mainloop()

asyncio.run(main())
