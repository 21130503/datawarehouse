from sqlalchemy import create_engine, MetaData
from databases import Database
from sqlalchemy.exc import SQLAlchemyError
import asyncio

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
        return username, password, email, name_db

    async def connectODBC(self):
        username, password, email, name_db = self.getAttr()
        DATABASE_URL = f"mssql+pyodbc://{username}:{password}@server/{name_db}?driver=ODBC+Driver+17+for+SQL+Server"

        try:
            engine = create_engine(DATABASE_URL)
            connection = engine.connect()
            print("Connected Successfully")
            connection.close()  # Đóng kết nối sau khi sử dụng
        except SQLAlchemyError as error:
            print(f"Error connecting to the database: {error}")
            send_mail = SendMail("danghuuquy10042003@gmail.com")
            send_mail.sendMail(subject='ERR', body='Error connecting to the database')

async def main():
    connect = Connect('config')
    await connect.connectODBC()  # Sử dụng await để gọi connectODBC
if __name__ == '__main__':
    asyncio.run(main())