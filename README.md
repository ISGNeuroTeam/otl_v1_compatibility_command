# pp_cmd_otl_v1
Postprocessing command "otl_v1"
## Description
Command runs otl query on old platform. 


### Arguments
- code - positional argument, text, required. Code to run on the old platform
- timeout - keyword argument, integer, not required, default is `default_job_timeout` value from command config file.
- cache_ttl - keyword argument, integer, not required, default is `default_request_cache_ttl` value from command config file.

### Usage example
```
query: otl_v1 <# makeresults count=4 | eval a=3 #>, timeout=15, cache_ttl=10

            _time  a
Index               
0      1680009770  3
1      1680009770  3
2      1680009770  3
3      1680009770  3

```


## Getting started
### Installing
1. Create virtual environment with post-processing sdk 
```bash
    make dev
```
That command  
- downloads [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
- creates python virtual environment with [postprocessing_sdk](https://github.com/ISGNeuroTeam/postprocessing_sdk)
- creates link to current command in postprocessing `pp_cmd` directory 

2. Configure `otl_v1` command. Example:  
```bash
    vi ./otl_v1/config.ini
```
Config example:  
```ini
[spark]
base_address = http://localhost
username = admin
password = 12345678

[caching]
# 24 hours in seconds
login_cache_ttl = 86400
# Command syntax defaults
default_request_cache_ttl = 100
default_job_timeout = 100
```

3. Configure storages for `readFile` and `writeFile` commands:  
```bash
   vi ./venv/lib/python3.9/site-packages/postprocessing_sdk/pp_cmd/readFile/config.ini
   
```
Config example:  
```ini
[storages]
lookups = /opt/otp/lookups
pp_shared = /opt/otp/shared_storage/persistent
```

### Run otl_v1
Use `pp` to run otl_v1 command:  
```bash
pp
Storage directory is /tmp/pp_cmd_test/storage
Commmands directory is /tmp/pp_cmd_test/pp_cmd
query: | otl_v1 <# makeresults count=100 #> |  otl_v1 <# makeresults count=1 #>
```
## Deploy
1. Unpack archive `pp_cmd_otl_v1` to postprocessing commands directory
2. Configure config.ini 