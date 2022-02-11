import json
import os
import random
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from functools import partial, lru_cache
from typing import List, Dict, Tuple

from pp_exec_env.base_command import pd
from pp_exec_env.schema import ddl_to_pd_schema


BASE_ADDRESS = ""


def get_ttl_hash(seconds=3600):
    """Return the same value withing `seconds` time period"""
    return round(time.time() / seconds)


@lru_cache(maxsize=3)
def login(username: str, password: str, cache_ttl: int) -> str:
    url = f"{BASE_ADDRESS}/api/auth/login"
    data = json.dumps({
        "username": username,
        "password": password
    }).encode()
    headers = {"Content-Type": "application/json"}

    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers)) as r:  # Make request
        result = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))
        cookie = r.headers.get('Set-Cookie')

    if result["status"] != "success":
        raise ValueError(f"Failed authentication: {json.dumps(result)}")

    return cookie


def make_job(request_data: Dict, username: str, cookie: str) -> Dict:
    additional_data = {
        "sid": f"{random.randint(1000000, 99999999)}",
        "username": username,
        "field_extraction": "false",
        "preview": "false"
    }
    data = {**request_data, **additional_data}  # Merge request_data and additional_data
    data = urllib.parse.urlencode(data).encode('ascii')  # urlencode data
    url = f"{BASE_ADDRESS}/api/makejob"

    request = urllib.request.Request(url, data)
    request.add_header("Cookie", cookie)

    with urllib.request.urlopen(request) as r:  # Make request
        result = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))

    if result["status"] != "success":
        raise ValueError(f"OTLv1 Makejob failed: {json.dumps(result)}")
    return result


def check_job(request_data: Dict, cookie: str) -> int:
    url = f"{BASE_ADDRESS}/api/checkjob?{urllib.parse.urlencode(request_data)}"
    request = urllib.request.Request(url)
    request.add_header("Cookie", cookie)

    cid = 0
    while True:
        with urllib.request.urlopen(request) as r:  # Make request
            result = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))

        if result["status"] == "failed":
            raise ValueError(result["error"])
        elif result["status"] == "success":
            cid = result["cid"]
            break
        else:
            pass
        time.sleep(0.5)

    return cid


@lru_cache(maxsize=100)
def get_result(cid: int, cookie: str, cache_ttl: int) -> List[str]:
    url = f"{BASE_ADDRESS}/api/getresult?cid={cid}"
    request = urllib.request.Request(url)
    request.add_header("Cookie", cookie)

    with urllib.request.urlopen(request) as r:  # Make request
        result = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))

    if result["status"] != "success":
        raise ValueError(f"OTLv1 GetResult failed: {json.dumps(result)}")

    return result["data_urls"]


def get_data(data_path: str, cookie: str) -> Tuple[bytes, str]:
    url = f"{BASE_ADDRESS}/{data_path}"
    request = urllib.request.Request(url)
    request.add_header("Cookie", cookie)

    with urllib.request.urlopen(request) as r:  # Make request
        result = r.read()
        encoding = r.info().get_param('charset') or 'utf-8'
    return result, encoding


def get_dataframe(paths: List[str], cookie: str) -> pd.DataFrame:
    _get_data = partial(get_data, cookie=cookie)

    # It will close itself even if alarm goes off
    with ThreadPoolExecutor(max_workers=min([len(paths), os.cpu_count()])) as executor:
        results = executor.map(_get_data, paths)

    results = list(results)

    ddl_index = [i for i, s in enumerate(paths) if '_SCHEMA' in s][0]
    ddl, encoding = results.pop(ddl_index)
    ddl = ddl.decode(encoding)

    data = ''.join([s.decode(e) for s, e in results])

    schema, ddl_schema = ddl_to_pd_schema(ddl)
    df = pd.read_json(data, orient="records", lines=True, dtype=schema, keep_default_dates=False)
    df.index.name = "Index"
    df.schema._initial_schema = ddl_schema
    return df
