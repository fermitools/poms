from typing import Any, Dict, List, Optional
import toml
import os

class OperationError(Exception):
    """Exception raised for errors in the execution of an operation.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self: Exception, message: str):
        self.message = message
        super().__init__(message)

class TConfig(dict):
        
    def __init__(self: 'TConfig', **with_values: Optional[Dict[str, Any]]):
        # Load the TOML file
        super().__init__()
        self._allowed_keys: List[str] = ["Shrek"]
        with open(os.environ["WEB_CONFIG"], 'r') as file:
            self._config = toml.load(file)
        
        self._config: Dict[str, Any] = self.format_if_needed(self._config, **with_values)
        self.update(self._config)
    
    def __setitem__(self: 'TConfig', key: Any, value: Any) -> None:
        """ Set an allowed key-value pair in the configuration file."""
        if key in self._allowed_keys and key not in self._config:
            self._config[key] = value
            self.update(self._config)
        else:
            raise OperationError(f"Operation denied: Key '{key}' is not permitted.")
    
    def __getitem__(self: 'TConfig', key: Any) -> Any:
        return super().__getitem__(key)
    
    def merge(self: 'TConfig',key, value):
        """ Alias for __setitem__ method"""
        self.__setitem__(key, value)
        
    def get_cherrypy_config(self: 'TConfig') -> Dict[str, Any]:
        """ Get the configuration file."""
        cherrypy_config = {
            '/': self._config["/"],
            "/static": self._config["/static"],
            "global": self._config["global"],
            "POMS": self._config["POMS"],
            "Databases": self._config["Databases"],
            #"Shrek": self._config["Shrek"],
            
        }
        return cherrypy_config
    
    def get(self: 'TConfig', section: str, field: Optional[str] = None, default=None, **format: Optional[Dict[str, Any]]) -> Any:
        """ Get a value from the configuration file.
        
        Args:
            section (str): The section in the configuration file.
            field (str): The field in the section.
            format (dict): A dictionary with placeholders to substitute in the value.
            
        """
        try:
            if not self._config.__contains__(section):
                if default:
                    return default
                raise ValueError(f"Section '{section}' not found in configuration file")
            if field and field not in self._config.__getitem__(section):
                if default:
                    return default
                raise ValueError(f"Field '{field}' not found in section '{section}'")
            value = self.__getitem__(section)
            if field:
                value = value[field]
            elif default:
                value = default
            value = self.format_if_needed(value, **format)
            return value
            
        except ValueError as e:
            raise e
        except Exception as e:
            raise e

    def format_if_needed(self: 'TConfig', data: Any, **format: Optional[Dict[str, Any]]):
        """ Recursively substitute environment variables in strings found in dict or list. """
        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = self.format_if_needed(value, **format)
        elif isinstance(data, list):
            data = [self.format_if_needed(item, **format) for item in data]
        elif isinstance(data, str):
            # Find and substitute all placeholders in the format ${VAR_NAME}
            for k_key, k_value in format.items():
                if "${%s}" % k_key in data:
                    data = data.replace("${%s}" % k_key , k_value)
        return data