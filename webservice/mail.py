import smtplib
from email.mime.text import MIMEText


class Mail:
    def __init__(self):
        self.server = "smtp.fnal.gov:25"
        self.sender = "poms@fnal.gov"

    def send(self, subj, msg, to):
        # Create a text/plain message
        msg = MIMEText(msg)

        msg['Subject'] = subj
        msg['From'] = self.sender
        msg['To'] = to

        s = smtplib.SMTP(self.server)
        s.sendmail(self.sender, [to], msg.as_string())
        s.quit()



if __name__ == '__main__':
    #from mail import Mail
    mail = Mail()
    mail.send(subj="my subject", msg="my message", to="mgheith@fnal.gov")
