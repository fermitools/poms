import smtplib
from email.mime.text import MIMEText


class Mail:
    def __init__(self, debug=0):
        self.server = "smtp.fnal.gov:25"
        self.sender = "poms@fnal.gov"
        self.debug = debug

    def send(self, subj, msg, to):
        # Create a text/plain message
        msg = MIMEText(msg)

        msg['Subject'] = subj
        msg['From'] = self.sender
        msg['To'] = to

        s = smtplib.SMTP(self.server)
        s.set_debuglevel(self.debug)
        s.sendmail(self.sender, [to], msg.as_string())
        s.quit()



if __name__ == '__main__':
    #from mail import Mail
    mail = Mail(debug=1)  #turn on optional debug mode default is off
    mail.send(subj="my subject", msg="my message", to="mgheith@fnal.gov")
