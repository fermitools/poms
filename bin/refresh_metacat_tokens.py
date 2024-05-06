import argparse
from email.policy import default
from math import e
import os
import toml

from datetime import datetime, timedelta
import logging
from data_dispatcher.api import DataDispatcherClient
from metacat.webapi import MetaCatClient


class ShrekTokenRefresh:
    def __init__(self) -> None:
        self.log_level = logging.getLevelName("INFO")
        self.debug = False
        self._supported_experiments = ['hypot', 'mu2e', 'dune']
        self._successful_refreshes = {}
        self._shrek_config = None
        self._env = "prod"
        self._define_parser()
        self._parse_args()
        self.refresh_experiments()
        self._exit()
        
    

    def _define_parser(self):
        doc = "Refreshes metacat and data-dispatcher tokens for participating experiments for POMS system."
        self.parser = argparse.ArgumentParser(description=doc)
        self.parser.add_argument("-d", "--debug", help="Enable debug mode", action="store_true")
        self.parser.add_argument("-l", "--log", help="Log level", default="INFO")
        self.parser.add_argument("-x", "--experiment", help="Specific experiment to refresh tokens for, refreshes tokens for all experiments if unspecified.", default=None)
        config_parser = self.parser.add_mutually_exclusive_group(required=False)
        config_parser.add_argument("-e", "--env", help="Uses default path to the shrek config file for the specified environment (dev,prod)", default="prod")
        config_parser.add_argument("-c", "--config", help="Path to shrek config file", default=None)

    def _parse_args(self):
        try:
            args = self.parser.parse_args()
            if args.debug:
                self.debug = True
            if args.log:
                self.log_level = logging.getLevelName(args.log)
            else:
                self.log_level = logging.getLevelName("INFO")
            if args.env:
                assert args.env in ["dev", "prod"], "Invalid environment specified, must be either 'dev' or 'prod'"
                self._env = args.env
                self._shrek_config = toml.load(f'/home/poms/private/shrek/config/{self._env}.shrek.toml')
            elif args.config:
                assert os.path.exists(args.config), "Specified config file does not exist"
                self._shrek_config = toml.load(args.config)
            if args.experiment:
                self._supported_experiments = [args.experiment]
            for experiment in self._supported_experiments:
                self._successful_refreshes[experiment] = {"metacat": False, "data_dispatcher": False}
        except Exception as e:
            error = str(e).replace(f"\n", "")
            if self.debug:
                print(f"POMS | Shrek Token Refresh | Exception: {error}")
            else:
                logging.log(self.log_level, f"POMS | Shrek Token Refresh | Exception: {error}")
            exit(1)
    
    def _exit(self):
        if len(self._supported_experiments) == 1:
            experiment = self._supported_experiments[0]
            if self._successful_refreshes[experiment]["metacat"] and self._successful_refreshes[experiment]["data_dispatcher"]:
                exit(0)
            else:
                exit(1)
        else:
            successful = 0.0
            for experiment in self._supported_experiments:
                if self._successful_refreshes[experiment]["metacat"]:
                    successful += 1.0
                if self._successful_refreshes[experiment]["data_dispatcher"]:
                    successful += 1.0
            success_rate = ((successful / (float(len(self._supported_experiments)) * 2)) * 100.0).__round__(1)
            if self.debug:
                print(f"Success Rate: {success_rate}%")
            else:
                logging.log(self.log_level, f"Success Rate: {success_rate}%")
            if success_rate > 50:
                exit(0)
            else:
                exit(1)


    def refresh_experiments(self):
        try:
            # Set new client
            for experiment in self._supported_experiments:
                try:
                    mc_server_url = self._shrek_config[experiment]["metacat"]["METACAT_SERVER_URL"]
                    mc_auth_server_url = self._shrek_config[experiment]["metacat"]["METACAT_AUTH_SERVER_URL"]
                    mc_token_file = self._shrek_config[experiment]["metacat"]["TOKEN_FILE"]
                    dd_server_url = self._shrek_config[experiment]["data_dispatcher"]["DATA_DISPATCHER_URL"]
                    dd_auth_server_url = self._shrek_config[experiment]["data_dispatcher"]["DATA_DISPATCHER_AUTH_URL"]
                    dd_token_file = self._shrek_config[experiment]["data_dispatcher"]["TOKEN_FILE"]
                    mc_client = MetaCatClient(server_url=mc_server_url, auth_server_url=mc_auth_server_url, token_library=mc_token_file)
                    dd_client = DataDispatcherClient(server_url=dd_server_url, auth_server_url=dd_auth_server_url, token_library=dd_token_file)
                    failed = 0
                    # Do metacat
                    try:
                        if self.debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Begin")
                            mc_client.login_x509("poms", cert=self._shrek_config["poms"]["POMS_CERT"], key=self._shrek_config["poms"]["POMS_KEY"])
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Success")
                            self._successful_refreshes[experiment]["metacat"] = True
                        else:
                            logging.log(self.log_level, f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Begin")
                            mc_client.login_x509("poms", cert=self._shrek_config["poms"]["POMS_CERT"], key=self._shrek_config["poms"]["POMS_KEY"])
                            logging.log(self.log_level, f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Status: Success")
                            self._successful_refreshes[experiment]["metacat"] = True
                    except Exception as e:
                        failed += 1
                        error = str(e).replace(f"\n", "")
                        if self.debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Exception: {error}")
                        else:
                            logging.error(f"POMS | Shrek Token Refresh | {experiment} | Token: Metacat | Exception: {error}")
                    # Do data dispatcher
                    try:
                        if self.debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Begin")
                            dd_client.login_x509("poms", cert=self._shrek_config["poms"]["POMS_CERT"], key=self._shrek_config["poms"]["POMS_KEY"])
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Success")
                            self._successful_refreshes[experiment]["data_dispatcher"] = True
                        else:
                            logging.log(self.log_level, f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Begin")
                            dd_client.login_x509("poms", cert=self._shrek_config["poms"]["POMS_CERT"], key=self._shrek_config["poms"]["POMS_KEY"])
                            logging.log(self.log_level, f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Status: Success")
                            self._successful_refreshes[experiment]["data_dispatcher"] = True
                    except Exception as e:
                        failed += 1
                        error = str(e).replace(f"\n", "")
                        if self.debug:
                            print(f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Exception: {error}")
                        else:
                            logging.error(f"POMS | Shrek Token Refresh | {experiment} | Token: Data Dispatcher | Exception: {error}")
                    
                except Exception as e:
                    error = str(e).replace(f"\n", "")
                    if self.debug:
                        print(f"POMS | Shrek Token Refresh | {experiment} | Exception: {error}")
                        print(f"\n")
                    else:
                        logging.error(f"POMS | Shrek Token Refresh | {experiment} | Exception: {error}")
        except Exception as e:
            error = str(e).replace(f"\n", "")
            if self.debug:
                print(f"POMS | Shrek Token Refresh | Exception: {error}")
            else:
                logging.error(f"POMS | Shrek Token Refresh | Exception: {error}")
        
            
if __name__ == "__main__":
    refresher = ShrekTokenRefresh()