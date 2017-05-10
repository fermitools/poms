import inspect
import logging

logger = logging.getLogger('cherrypy.error')


def logstartstop(function):
    def wrapper(*args,**kwargs):
        logger.info("poms_service.%s Method Start" % function.__name__)
        try:
            return function(*args,**kwargs)
        finally:
            logger.info("poms_service.%s Method End" % function.__name__)
    return wrapper

def log(message,level=logging.INFO):
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    source = calframe[1][1]
    source = source[source.rfind('/')+1:]
    source = "%s.%s" % (source[:source.rfind('.')],calframe[1][3])
    logger.log(level,"%s: %s" % (source, message))
