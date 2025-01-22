import configparser
import os

class ConfigParser():
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.environ["WEB_CONFIG"])
        
    
    def get(self, section, option, *, raw=False, vars=None, fallback=None):
        value = self.config.get(section, option, raw=raw, vars=vars, fallback=fallback)
        return value.strip('"') if value else value