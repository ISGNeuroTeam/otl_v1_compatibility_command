import logging
import signal
import urllib.error
from timeit import default_timer as timer
from typing import Dict

from pp_exec_env.base_command import BaseCommand, Syntax, Rule, pd
from . import api


def timeout_handler(signum, frame):
    raise TimeoutError("OTLv1 request timeout!")


def make_request(username: str, password: str, data: Dict, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Authentication in progress")
    cookie = api.login(username, password, api.get_ttl_hash(3600 * 24))  # 24 hours of login caching

    try:
        logger.info("Creating an OTLv1 Job")
        api.make_job(data, username, cookie)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            logger.warning("Error 401 during makejob request, perhaps a cache miss")
            logger.warning("Reattempting with no cache")

            api.login.cache_clear()
            logger.info("Authentication in progress")
            cookie = api.login(username, password, api.get_ttl_hash(3600 * 24))

            logger.info("Creating an OTLv1 Job")
            api.make_job(data, username, cookie)
        else:
            logging.error(f"Unknown HTTP Error: {e.__str__()}")
            raise e

    logger.info("Waiting for results")
    cid = api.check_job(data, cookie)

    logger.info("Fetching results info")
    results_paths = api.get_result(cid, cookie, api.get_ttl_hash(data["cache_ttl"]))  # An hour of results paths caching

    try:
        logger.info("Preparing the DataFrame")
        df = api.get_dataframe(results_paths, cookie)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning("Error 404 during data fetch, perhaps a cache miss")
            logger.warning("Reattempting with no cache")

            api.get_result.cache_clear()
            logger.info("Fetching results info")
            results_paths = api.get_result(cid, cookie, api.get_ttl_hash(data["cache_ttl"]))

            logger.info("Preparing the DataFrame")
            df = api.get_dataframe(results_paths, cookie)
        else:
            logging.error(f"Unknown HTTP Error: {e.__str__()}")
            raise e
    return df


class OTLV1Command(BaseCommand):
    syntax = Syntax([Rule(name="code", required=True, input_types=["inline", "string"]),
                     Rule(name="timeout", required=False, input_types=["integer"], type="kwarg"),
                     Rule(name="tws", required=False, type="kwarg"),
                     Rule(name="twf", required=False, type="kwarg"),
                     Rule(name="cache_ttl", required=False, type="kwarg")],
                    use_timewindow=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        api.BASE_ADDRESS = self.config["spark"]["base_address"]
        username = self.config["spark"]["username"]
        password = self.config["spark"]["password"]
        timeout = self.get_arg("timeout").value or self.config["caching"].getint("default_job_timeout")

        request_data = {
            "original_otl": self.get_arg("code").value,
            "tws": self.get_arg("tws").value or 0,
            "twf": self.get_arg("twf").value or 0,
            "cache_ttl": self.get_arg("cache_ttl").value or self.config["caching"].getint("default_request_cache_ttl"),
            "timeout": timeout
        }

        signal.signal(signal.SIGALRM, timeout_handler)  # register alarm handler
        signal.alarm(timeout)  # set as alarm

        start_time = timer()
        # if this is too long, TimeoutError will be raised
        df = make_request(username, password, request_data, self.logger)
        end_time = timer()

        signal.alarm(0)  # Cancel timer

        self.logger.info(f"Request took {end_time - start_time:.4f} seconds")
        return df
