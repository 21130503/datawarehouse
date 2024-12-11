import asyncio
from connect import Connect
from writeLog import writeLog


async def main():
    # 1. Connect to the database
    connect = Connect()

    # 2. Access File Config
    connection = await connect.connectODBC()
    if connection:
        cursor = connection.cursor()
        cursor.execute('select Id,LocationFile from ConfigFile')
        result = cursor.fetchall()
        # 3. get Fied
        location_files = [row[1] for row in result]
        Id = [row[0] for row in result]
        try:
            # Thực thi stored procedure
            cursor.execute("EXEC ImportAccidentDalyData @path = ?", (location_files[0],))
            connection.commit()  # Commit the transaction nếu cần
            # Ghi log thành công
            await writeLog(Id[0], "ER", "Load Staging Successfully")
        except Exception as error:
            # Xử lý ngoại lệ khi thực thi stored procedure
            error_message = f"Load Staging Failure: {str(error)}"
            print(error_message)
            await writeLog(Id[0], "ERR", error_message)
        finally:
            connection.close()  # Close the connection after use


# Run the async main function
if __name__ == '__main__':
    asyncio.run(main())
