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
            send_mail.sendMail("Thành công", 'Load to staging oke')
        except Exception as e:
            print(f"Lỗi xảy ra: {e}" )
            send_mail.sendMail("Thất bại", "Load to staging failure")
        finally:
            connection.close()
