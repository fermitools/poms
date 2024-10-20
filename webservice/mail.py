import smtplib
from email.mime.text import MIMEText
#import configparser
import os
import poms.webservice.logit as logit
from toml_parser import TConfig


class Mail:
    def __init__(self):
        self.server, self.sender, self.debug = self.__get_smtp_info()

    def __get_smtp_info(self):
        logit.log("Mail: in %s" % os.environ["POMS_DIR"])
        #config = configparser.ConfigParser()
        #config.read("../webservice/poms.ini")
        config = TConfig()
        server = config.get("smtp", "server").strip('"')
        sender = config.get("smtp", "sender").strip('"')
        debug = int(config.get("smtp", "debug"))
        return (server, sender, debug)

    def send(self, subj, msg, to):
        # Create a text/plain message
        msg = MIMEText(msg)

        msg["Subject"] = subj
        msg["From"] = self.sender
        msg["To"] = to

        s = None
        try:
            s = smtplib.SMTP(self.server)
            s.set_debuglevel(self.debug)
            s.sendmail(self.sender, [to], msg.as_string())
        except Exception as e:
            print("oops: %s" % e)
        finally:
            if s:
                s.quit()


if __name__ == "__main__":
    # from mail import Mail
    mail = Mail()
    mail.send(subj="my subject", msg="my message", to="podstvkv@fnal.gov")
