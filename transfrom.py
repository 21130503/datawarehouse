from connect import Connect
import  asyncio
# Connect
async def connect():
    connect = Connect()
    connection = await connect.connectODBC()
    return connection

# Gọi các proc với xử lý lỗi cụ thể
# Gọi các proc với xử lý lỗi cụ thể
async def call_proc(produce):
    connection = None
    cursor = None
    try:
        connection = await connect()  # Kết nối async
        if connection:
            cursor = connection.cursor()  # Không sử dụng await
            try:
                cursor.execute(produce)  # Không sử dụng await
                connection.commit()
                # Ghi log thành công
                cursor.execute(f"exec writeLog {1001}, 'INFO', '{produce} executed successfully'")
                connection.commit()
                print(f"Procedure '{produce}' executed successfully.")
            except Exception as e:
                # Bắt lỗi khi thực thi procedure
                error_message = f"Error executing procedure '{produce}': {str(e)}"
                print(error_message)
                if connection and cursor:
                    cursor.execute(f"exec writeLog {1002}, 'ERR', '{error_message}'")
                    connection.commit()
    except Exception as db_error:
        print(f"Database connection error: {str(db_error)}")
    finally:
        # Đóng kết nối
        if cursor:
            cursor.close()
        if connection:
            connection.close()

async def test_call_proc():
    print("Testing call_proc...")
    await call_proc("exec [dbo].[proc_addDimLocation] ")
    await call_proc("exec [dbo].[proc_addDimWeather]")
    await call_proc("exec [dbo].[proc_addDimSerious]")
    await call_proc("exec [dbo].[proc_addDimIncidentType]")
    await call_proc("exec [dbo].[proc_addDimCreatedAt_1]")
    await call_proc("exec [dbo].[proc_addFact]")

# Chạy test
if __name__ == "__main__":
    asyncio.run(test_call_proc())