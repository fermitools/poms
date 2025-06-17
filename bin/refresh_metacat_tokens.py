import toml

from datetime import datetime, timedelta
import logging
from data_dispatcher.api import DataDispatcherClient
from metacat.webapi import MetaCatClient

_supported_experiments = ['hypot', 'mu2e', 'dune']


ERROR = 40
INFO = 20
debug = False

def refresh_experiments():
        try:
            # Set new client
            shrek_config = toml.load('/home/poms/poms/webservice/config/shrek.toml')
            for experiment in _supported_experiments:
                try:
                    mc_server_url = shrek_config[experiment]["metacat"]["METACAT_SERVER_URL"]
                    mc_auth_server_url = shrek_config[experiment]["metacat"]["METACAT_AUTH_SERVER_URL"]
                    mc_token_file = shrek_config[experiment]["metacat"]["TOKEN_FILE"]
                    dd_server_url = shrek_config[experiment]["data_dispatcher"]["DATA_DISPATCHER_URL"]
                    dd_auth_server_url = shrek_config[experiment]["data_dispatcher"]["DATA_DISPATCHER_AUTH_URL"]
                    dd_token_file = shrek_config[experiment]["data_dispatcher"]["TOKEN_FILE"]
                    mc_client = MetaCatClient(server_url=mc_server_url, auth_server_url=mc_auth_server_url, token_library=mc_token_file)
                    dd_client = DataDispatcherClient(server_url=dd_server_url, auth_server_url=dd_auth_server_url, token_library=dd_token_file)
                    failed = 0
                    # Do metacat
                    try:
                        if debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Begin")
                            mc_client.login_x509("poms", cert=shrek_config["poms"]["POMS_CERT"], key=shrek_config["poms"]["POMS_KEY"])
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Success")
                        else:
                            logging.log(INFO, f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Begin")
                            mc_client.login_x509("poms", cert=shrek_config["poms"]["POMS_CERT"], key=shrek_config["poms"]["POMS_KEY"])
                            logging.log(INFO, f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Success")
                    except Exception as e:
                        failed += 1
                        error = str(e).replace(f"\n", "")
                        if debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Exception: {error}")
                        else:
                            logging.log(ERROR, f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Exception: {error}")
                    # Do data dispatcher
                    try:
                        if debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Begin")
                            dd_client.login_x509("poms", cert=shrek_config["poms"]["POMS_CERT"], key=shrek_config["poms"]["POMS_KEY"])
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Success")
                        else:
                            logging.log(INFO, f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Begin")
                            dd_client.login_x509("poms", cert=shrek_config["poms"]["POMS_CERT"], key=shrek_config["poms"]["POMS_KEY"])
                            logging.log(INFO, f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Success")
                    except Exception as e:
                        failed += 1
                        error = str(e).replace(f"\n", "")
                        if debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Exception: {error}")
                        else:
                            logging.log(ERROR, f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Exception: {error}")
                    
                except Exception as e:
                    error = str(e).replace(f"\n", "")
                    if debug:
                        print(f"POMS | Shrek Token Refresh | {experiment} | Exception: {error}")
                        print(f"\n")
                    else:
                        logging.log(ERROR, f"POMS | Shrek Token Refresh | {experiment} | Exception: {error}")
                
                
            
        except Exception as e:
            error = str(e).replace(f"\n", "")
            if debug:
                print(f"POMS | Shrek Token Refresh | Exception: {error}")
            else:
                logging.error(ERROR, f"POMS | Shrek Token Refresh | Exception: {error}")
            
        
refresh_experiments()