import pathlib
import os.path
from joblib.parallel import cpu_count

import gevent.monkey

from config.setting import LOG_PATH


gevent.monkey.patch_all()

bind = '0.0.0.0:5000'

workers = cpu_count()
worker_class = 'gevent'
worker_connections = 2000

backlog = 2048
max_requests = 0
max_requests_jitter = 0
timeout = 2000
graceful_timeout = 1800
keepalive = 30

pidfile = 'gunicorn.pid'

loglevel = 'info'
accesslog = os.path.join(LOG_PATH, 'gunicorn_access.log')
errorlog = os.path.join(LOG_PATH, 'gunicorn_error.log')

pathlib.Path(LOG_PATH).mkdir(parents=True, exist_ok=True)
pathlib.Path(accesslog).touch()
pathlib.Path(errorlog).touch()

reload = True

x_forwarded_for_header = 'X-FORWARDED-FOR'