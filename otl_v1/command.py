import logging
import signal
import urllib.error
from timeit import default_timer as timer
from typing import Dict, Callable

import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from . import api

TOTAL_STAGES = 6


def timeout_handler(signum, frame):
    raise TimeoutError("OTLv1 request timeout!")


def make_request(username: str, password: str, login_cache_ttl: int, data: Dict, logger: logging.Logger,
                 log_progess: Callable) -> pd.DataFrame:
    logger.info("Authentication in progress")
    stage = 0
    log_progess("Authentication in progress", stage=(stage := stage + 1), total_stages=TOTAL_STAGES)
    cookie = api.login(username, password, api.get_ttl_hash(login_cache_ttl))  # 24 hours of login caching

    log_progess("Creating an OTLv1 Job", stage=(stage := stage + 1), total_stages=TOTAL_STAGES)
    try:
        logger.info("Creating an OTLv1 Job")
        api.make_job(data, username, cookie)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            logger.warning("Error 401 during makejob request, perhaps a cache miss")
            logger.warning("Reattempting with no cache")

            api.login.cache_clear()
            logger.info("Authentication in progress")
            cookie = api.login(username, password, api.get_ttl_hash(login_cache_ttl))

            logger.info("Creating an OTLv1 Job")
            api.make_job(data, username, cookie)
        else:
            logging.error(f"Unknown HTTP Error: {e.__str__()}")
            raise e

    log_progess("Waiting for results", stage=(stage := stage + 1), total_stages=TOTAL_STAGES)
    logger.info("Waiting for results")
    cid = api.check_job(data, cookie)

    log_progess("Fetching results info", stage=(stage := stage + 1), total_stages=TOTAL_STAGES)
    logger.info("Fetching results info")
    results_paths = api.get_result(cid, cookie, api.get_ttl_hash(data["cache_ttl"]))  # An hour of results paths caching

    try:
        log_progess("Preparing the DataFrame", stage=(stage := stage + 1), total_stages=TOTAL_STAGES)
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
    syntax = Syntax([Positional("code", required=True, otl_type=OTLType.TEXT),
                     Keyword("timeout", required=False, otl_type=OTLType.INTEGER),
                     Keyword("cache_ttl", required=False, otl_type=OTLType.INTEGER)])
    use_timewindow = True

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        api.BASE_ADDRESS = self.config["spark"]["base_address"]
        username = self.config["spark"]["username"]
        password = self.config["spark"]["password"]
        timeout = self.get_arg("timeout").value or self.config["caching"].getint("default_job_timeout")

        request_data = {
            "original_otl": self.get_arg("code").value,
            "tws": self.get_arg("earliest").value or 0,
            "twf": self.get_arg("latest").value or 0,
            "cache_ttl": self.get_arg("cache_ttl").value or self.config["caching"].getint("default_request_cache_ttl"),
            "timeout": timeout
        }

        signal.signal(signal.SIGALRM, timeout_handler)  # register alarm handler
        signal.alarm(timeout)  # set as alarm

        start_time = timer()
        # if this is too long, TimeoutError will be raised
        df = make_request(username,
                          password,
                          self.config["caching"].getint("login_cache_ttl"),
                          request_data,
                          self.logger,
                          self.log_progress)
        end_time = timer()

        signal.alarm(0)  # Cancel timer

        self.logger.info(f"Request took {end_time - start_time:.4f} seconds")
        return df
