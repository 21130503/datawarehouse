from sqlalchemy import create_engine, MetaData
from databases import Database
from sqlalchemy.exc import SQLAlchemyError
import asyncio
import pypyodbc as odbc
from sendMail import SendMail


class Connect :
    def __init__(self, fileName):
        self.fileName = fileName
    def getAttr(self):
        attr = {}
        with open(self.fileName,'r') as f:
            for line in f:
                k, v  = line.strip().split('=')
                attr[k.strip()] = v.strip()
        username = attr.get('username', '')
        password = attr.get('password', '')
        email = attr.get('email', '')
        name_db = attr.get('name_db', '')
        driver_name = attr.get('driver_name', '')
        server_name = attr.get('server_name', '')
        return username, password, email, name_db, driver_name, server_name

    async def connectODBC(self):
        username, password, email, name_db, driver_name, server_name = self.getAttr()
        connectionString = f"""
                                DRIVER = {{{driver_name}}};
                                SERVER = {server_name};
                                DATABASE = {name_db};
                                Trust_connection = yes;
                            """
        # DATABASE_URL = f"mssql+pyodbc://{'huuquy'}:{'huuquy2003'}@server/{'dw'}?driver=ODBC+Driver+17+for+SQL+Server"

        try:
            connection = odbc.connect(connectionString)
            print("Connected Successfully")
            connection.close()  # Đóng kết nối sau khi sử dụng
        except SQLAlchemyError as error:
            print(f"Error connecting to the database: {error}")
            send_mail = SendMail(email)
            send_mail.sendMail(subject='ERR-CONNECT DB', body='Error connecting to the database')

async def main():
    connect = Connect('config')
    await connect.connectODBC()  # Sử dụng await để gọi connectODBC
if __name__ == '__main__':
    asyncio.run(main())