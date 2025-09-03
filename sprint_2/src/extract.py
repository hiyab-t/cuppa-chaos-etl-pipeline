import csv
import io
import logging


LOGGER = logging.getLogger()

LOGGER.setLevel(logging.INFO)

def extract(body_text):
    LOGGER.info('Extract: starting...')

    reader = csv.reader(io.StringIO(body_text), delimiter=',')

    data = [row for row in reader if row]

    LOGGER.info(f'Extract: done: rows{len(data)}')

    return data

