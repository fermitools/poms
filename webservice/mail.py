import smtplib
from email.mime.text import MIMEText
import ConfigParser


class Mail:
    def __init__(self):
        self.server, self.sender, self.debug = self.__get_smtp_info()

    def __get_smtp_info(self):
        config = ConfigParser.ConfigParser()
        config.read('../webservice/poms.ini')
        server = config.get('smtp', 'server').strip('"')
        sender = config.get('smtp', 'sender').strip('"')
        debug = config.getint('smtp', 'debug')
        return (server, sender, debug)

    def send(self, subj, msg, to):
        # Create a text/plain message
        msg = MIMEText(msg)

        msg['Subject'] = subj
        msg['From'] = self.sender
        msg['To'] = to

        try:
            s = smtplib.SMTP(self.server)
            s.set_debuglevel(self.debug)
            s.sendmail(self.sender, [to], msg.as_string())
        except Exception, e:
            print 'oops: %s' % e
        finally:
            s.quit()


if __name__ == '__main__':
    #from mail import Mail
    mail = Mail()
    mail.send(subj="my subject", msg="my message", to="mgheith@fnal.gov")