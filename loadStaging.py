import asyncio
from connect import Connect


async def main():
    # 1. Connect to the database
    connect = Connect()

    # 2. Get the database connection
    connection = await connect.connectODBC()
    if connection:
        cursor = connection.cursor()
        cursor.execute('select LocationFile from ConfigFile')
        result = cursor.fetchall()
        location_files = [row[0] for row in result]
        try:
            cursor.execute("EXEC ImportAccidentDalyData @path = ?", (location_files[0],))
            connection.commit()  # Commit the transaction if needed
            print("Data imported successfully.")
        except Exception as error:
            print(f"Error calling stored procedure: {error}")
        finally:
            connection.close()  # Close the connection after use


# Run the async main function
if __name__ == '__main__':
    asyncio.run(main())
