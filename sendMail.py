from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

class SendMail():
    def __init__(self, email):
        self.email = email

    def sendMail(self, subject, body):
        sender = '21130503@st.hcmuaf.edu.vn'
        password = 'gyvzbtqzyjoqveyn'
        message = MIMEMultipart()
        message["From"] = sender
        message["To"] = self.email
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()  # Secure the connection
                server.login(sender, password)  # Login
                server.sendmail(sender, str(self.email), message.as_string())  # Send email
            print("Email sent successfully!")
        except Exception as e:
            print(f"Error: {e}")


