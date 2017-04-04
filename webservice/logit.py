import inspect
import logging

NOTSET   = "NOTSET"
DEBUG    = "DEBUG"
INFO     = "INFO"
WARNING  = "WARNING"
ERROR    = "ERROR"
CRITICAL = "CRITICAL"

logger = logging.getLogger('cherrypy.error')


def logstartstop(function):
    def wrapper(*args,**kwargs):
        logger.info("poms_service.%s Method Start" % function.__name__)
        try:
            return function(*args,**kwargs)
        finally:
            logger.info("poms_service.%s Method End" % function.__name__)
    return wrapper

# examples of calling:
# log("your message here")
# log(ERROR, "your message here")
def log(*args):
    if len(args) == 1:
        __logmess(INFO,args[0])
    else:
        __logmess(args[0],args[1])


def setlevel(level="INFO", loggers=["cherrypy.error", "cherrypy.access", "sqlalchemy.engine"]):
    # loggers: cherrypy.error, cherrypy.access, sqlalchemy.engine (there are more for sqlalchemy)
    new_level = __getlevel(level)
    for da_logger in loggers:
        logging.getLogger(da_logger).setLevel(new_level)
        logger.critical("%s level set to: %s" % (da_logger,level))


def __logmess(level=INFO, message="message not supplied to logit.__logmess",da_frame=2):
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    source = calframe[da_frame][1]
    source = source[source.rfind('/')+1:]
    source = "%s.%s" % (source[:source.rfind('.')],calframe[1][3])
    logger.log(__getlevel(level), "%s: %s" % (source, message) )


def __getlevel(level):
    # support use of logging values passed in
    if isinstance( level, ( int, long ) ):
        return level
    if level == "NOTSET":
        return logging.NOTSET
    if level == "DEBUG":
        return logging.DEBUG
    if level == "INFO":
        return logging.INFO
    if level == "WARNING":
        return  logging.WARNING
    if level == "ERROR":
        return logging.ERROR
    if level == "CRITICAL":
        return logging.CRITICAL
