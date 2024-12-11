from connect import Connect
from sendMail import SendMail


async  def writeLog(idConfig,status, content):
    print(f"Run write with {idConfig}" )
    connect = Connect()
    connection = await connect.connectODBC()
    send_mail = SendMail('danghuuquy10042003@gmail.com')
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute(f"exec writeLog {idConfig}, '{status}', '{content}'")
            connection.commit()
            send_mail.sendMail(f"{status}-Load To Staging", content)
        except Exception as e:
            print(f"Lỗi xảy ra: {e}" )
        finally:
            connection.close()
