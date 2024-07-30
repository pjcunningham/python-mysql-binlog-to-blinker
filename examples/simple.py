# -*- coding: utf-8 -*-
__author__ = 'tarzan'


import logging
import mysqlbinlog2blinker
from mysqlbinlog2blinker import signals
from environs import Env


def setup_logging(level=None):
    import logging.config
    import logging
    import os

    _ini = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        'logging.ini',
    ))
    logging.config.fileConfig(_ini, disable_existing_loggers=False)
    if level:
        logging.root.setLevel(level)


setup_logging(logging.INFO)


_logger = logging.getLogger(__name__)


@signals.on_binlog_position
def on_binlog_position(data):
    print(f"Bin Log File: {data[0]}")
    print(f"Bin Log Position: {data[1]}")


@signals.on_rows_inserted
def on_rows_inserted(table, rows, meta):
    print('ROWS INSERTED')
    print(table)
    print(meta)
    for row in rows:
        print(row)


@signals.on_rows_updated
def on_rows_updated(table, rows, meta):
    print('ROWS UPDATED')
    print(table)
    print(meta)
    for row in rows:
        print(row)


@signals.on_rows_deleted
def on_rows_deleted(table, rows, meta):
    print('ROWS DELETED')
    print(table)
    print(meta)
    for row in rows:
        print(row)


def main():
    env = Env()
    env.read_env()
    mysql_settings = {
        'host': env.str('HOST'),
        'port': env.int('PORT'),
        'user': env.str('USER'),
        'password': env.str('PASSWORD'),
    }
    mysqlbinlog2blinker.start_replication(mysql_settings)


if __name__ == '__main__':
    main()
