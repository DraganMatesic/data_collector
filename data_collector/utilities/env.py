import os
import dotenv
import hashlib
from pathlib import Path
from datetime import datetime
from data_collector.utilities import functions


def check(logger, extra=None, idx=0):
    # code block necessary for standalone function
    start_time = datetime.now()
    extra = functions.make_extra(__file__, extra=extra, function_name='check')
    extra.update({'start_time': start_time, 'function_no': idx})
    logger.info('executing check', extra=extra)

    # main code block
    logger.debug('checking dcload environment variable', extra=extra)
    dc_load = os.getenv('dcload')
    if dc_load is None:
        logger.debug('standalone - loading environment variables from .env', extra=extra)
        base_dir = Path(__file__).resolve().parent.parent.parent
        env_file = os.path.join(base_dir, 'data_collector', '.env')
        dotenv.load_dotenv(env_file, override=True)
    else:
        logger.debug('dcload environment variable exists - load O.K', extra=extra)

    # code block necessary for standalone function ending
    extra.update(functions.function_end(start_time))
    logger.info('finished executing check', extra=extra)