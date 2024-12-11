from sqlalchemy import create_engine, MetaData
from databases import Database
import asyncio
import pypyodbc as odbc
from sendMail import SendMail


class Connect:

    def getAttr(self, fileName):
        attr = {}
        with open(fileName, 'r') as f:
            for line in f:
                k, v = line.strip().split('=')
                attr[k.strip()] = v.strip()

        username = attr.get('username', '')
        password = attr.get('password', '')
        email = attr.get('email', '')
        name_db = attr.get('name_db', '')
        driver_name = attr.get('driver_name', '')
        server_name = attr.get('server_name', '')
        return username, password, email, name_db, driver_name, server_name

    async def connectODBC(self):
        username, password, email, name_db, driver_name, server_name = self.getAttr('config')

        # Constructing the ODBC connection string
        connectionString = f"DRIVER={{{driver_name}}};SERVER={server_name};DATABASE={name_db};Trusted_Connection=yes"

        try:
            connection = odbc.connect(connectionString)
            print("Connected Successfully")
            return connection
        except Exception as error:
            print(f"Error connecting to the database: {error}")
            send_mail = SendMail(email)
            send_mail.sendMail(subject='ERR-CONNECT DB', body='Error connecting to the database')


async def main():
    connect = Connect()
    await connect.connectODBC()


if __name__ == '__main__':
    asyncio.run(main())
