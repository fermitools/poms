LOG_CONF = {
    'version': 1,

    'formatters': {
        'void': {
            'format': ''
            },
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        'error': {
            'format': '%(asctime)s <%(process)d.%(thread)d> %(message)s'
            },
        'access': {
            'format': '<%(process)d> %(message)s'
            },
        },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'error',
            'stream': 'ext://sys.stdout'
            },
        'cherrypy_console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'error',
            'stream': 'ext://sys.stdout'
            },
        'cherrypy_access': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'access',
            'stream': 'ext://sys.stdout'
            },
        'cherrypy_error': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'error',
            'stream': 'ext://sys.stderr'
            },
        },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO'
            },
        'cherrypy.access': {
            'handlers': ['cherrypy_access'],
            'level': 'INFO',
            'propagate': False
            },
        'cherrypy.error': {
            'handlers': ['cherrypy_error'],
            'level': 'INFO',
            'propagate': False
            },
        'sqlalchemy.engine': {
            'handlers': ['cherrypy_error'],
            'level': 'INFO',
            'propagate': False
            },
        }
    }
