import asyncio
from connect import Connect
from crawl import connection
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
            cursor.execute("EXEC ImportAccidentDalyData @path = ?", (location_files[0],))
            connection.commit()  # Commit the transaction if needed
            await writeLog(Id[0], "ER","Load Staging Successfully")
        except Exception as error:
            await writeLog(Id[0], "ERR","Load Staging Failure")

        finally:
            connection.close()  # Close the connection after use


# Run the async main function
if __name__ == '__main__':
    asyncio.run(main())
