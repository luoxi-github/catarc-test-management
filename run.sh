#!/usr/bin/env bash

/usr/local/catarc/python/bin/gunicorn manage:app -c gunicorn.conf.py --preload