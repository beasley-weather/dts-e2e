import os
import sqlite3
import threading
import time

import click
from werkzeug.serving import make_server

import dts_client
from dts_server.app import create_app
import weewx_orm

SOURCE_DATABASE_FILENAME = 'source.db'
TARGET_DATABASE_FILENAME = 'target.db'

SERVER_PORT = 22322
SERVER_ADDRESS = f'http://localhost:{SERVER_PORT}'
SERVER_API_PATH = SERVER_ADDRESS + '/data'

NUM_INTERVALS = 3
INTERVAL = 4

T_START = int(time.time())
T_END = T_START + (INTERVAL * NUM_INTERVALS + INTERVAL) # Additional interval to ensure
                                                        # all the data has transfered

print('Start time:', T_START)
print('End time:', T_END)

class ClientThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        INTERVAL_STAR_TIME = T_END
        self.client = dts_client.create_client(SERVER_API_PATH,
                                               SOURCE_DATABASE_FILENAME,
                                               INTERVAL,
                                               INTERVAL_STAR_TIME)

    def run(self):
        self.client.start()

    def stop(self):
        self.client.stop()


class ServerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.app = create_app(TARGET_DATABASE_FILENAME)
        self.server = make_server('127.0.0.1', SERVER_PORT, self.app)

    def run(self):
        self.server.serve_forever()

    def stop(self):
        self.server.shutdown()


def test_data_generator():
    return [(i, i) for i in range(T_START, T_START + INTERVAL * 3)]


def create_source_database():
    sql_create_table = open('archive_schema.sql').read()
    conn = sqlite3.connect(SOURCE_DATABASE_FILENAME)
    conn.execute(sql_create_table)
    conn.close()


def populate_source_database():
    conn = sqlite3.connect(SOURCE_DATABASE_FILENAME)
    conn.executemany('''
        insert into archive (dateTime, usUnits, interval, outTemp)
               values (?, 0, 0, ?)
    ''', test_data_generator())
    conn.commit()
    conn.close()


def test_target_database():
    conn = sqlite3.connect(TARGET_DATABASE_FILENAME)
    cur = conn.cursor()
    cur.execute('select dateTime, outTemp from archive');
    data = cur.fetchall()

    actualData = test_data_generator()

    ## FIXME Output a message indicating that the test passed or failed
    failed = False
    for idx, dataPair in enumerate(data):
        test_1 = assertEqual(dataPair[0], actualData[idx][0])
        test_2 = assertEqual(dataPair[1], actualData[idx][1])
        if not failed:
            failed = test_1 and test_2

    if not failed:
        print('Data has been successfully transfered between source.db and target.db')


def assertEqual(a, b):
    if a != b:
        print(f'Assertion failed: Expected {a} to be {b}')

def cleanup():
    try:
        os.remove(SOURCE_DATABASE_FILENAME)
        os.remove(TARGET_DATABASE_FILENAME)
    except FileNotFoundError as exc:
        raise


@click.command()
@click.option('--debug/--no-debug', default=False, help='Don\'t clean up databases')
def main(debug):
    try:
        try:
            cleanup()
        except FileNotFoundError:
            pass

        create_source_database()
        populate_source_database()

        clientThread = ClientThread()
        serverThread = ServerThread()

        clientThread.start()
        serverThread.start()

        time.sleep(T_END - time.time())

        clientThread.stop()
        serverThread.stop()

        clientThread.join()
        serverThread.join()

        test_target_database()
    except sqlite3.OperationalError as exc:
        raise
    except Exception as exc:
        raise
    finally:
        if not debug:
            cleanup()


if __name__ == '__main__':
    main()
