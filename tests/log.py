from logging.config import dictConfig


def configure():
    dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s %(levelname)-8s %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level': 'ERROR',
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'ERROR',
                'propagate': True
            },
        }
    })
