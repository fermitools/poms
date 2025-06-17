import inspect
from datetime import tzinfo, timedelta, datetime
import logging

LOGIT = logging.getLogger()
ZERO = timedelta(0)

class UTC(tzinfo):
    """UTC"""
    def utcoffset(self, dt):
        return ZERO
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return ZERO

utc = UTC()

# Global variables to track the current run    
_run_number = 0
_run_start = datetime.now(utc)

_started = False
_post_init = False

def set_run_number(number):
    global _run_number
    _run_number = number

def set_run_start(time):
    global _run_start
    _run_start = time

def current_run_info():
    now = datetime.now(utc)
    run_info = {
        "run_number": "pending",
        "timestamp": now.isoformat(),
    }
    global _started, _post_init, _run_number, _run_start
    if not _started:
        _started = True
        run_info["status"] = "Initializing Submission Agent"
        LOGIT.log(logging.INFO, run_info)
    
    if _run_number and isinstance(_run_number, int) and _run_number > 0:
        if not _post_init:
            _post_init = True
            run_info["status"] = "Submission Agent Finished Initializing"
            LOGIT.log(logging.INFO, run_info)
        run_info["run_number"] = _run_number
    else:
        run_info["run_number"] = "pending"
    
    elapsed = now - _run_start
    hours, remainder = divmod(elapsed.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = elapsed.microseconds // 1000
    if hours:
        run_info["runtime"] = f"{hours} hours, {minutes} minutes, {seconds} seconds"
    elif minutes:
        run_info["runtime"] = f"{minutes} minutes, {seconds} seconds"
    else:
        run_info["runtime"] = f"{seconds}.{milliseconds} seconds"
    return run_info


def record_queue_log(message, level="info", **kwargs):
    log_entry = {
        "service": "Agent Queue",
        "message": message,
        "level": level
    }
    log_entry.update(current_run_info())
    try:
        log_entry["class"] = inspect.stack()[1][0].f_locals["self"].__class__.__name__
        log_entry["function"] = inspect.stack()[1].function
    except:
        log_entry["class"] = "Unknown"
        log_entry["function"] = "Unknown"
    
    if kwargs:
        log_entry.update(kwargs)
            
    if level == "debug":
        LOGIT.debug(log_entry)
    if level == "info":
        LOGIT.info(log_entry)
    elif level == "error":
        LOGIT.error(log_entry)
    elif level == "exception":
        LOGIT.exception(log_entry)
    

def get_status(entry):
    """
        given a dictionary from the Landscape service,
        return the status for our submission
    """
    if entry["done"] and entry["failed"] * 2 >= entry["completed"]:
        return "Failed"
    # Only consider a submission as cancelled if every job within it is cancelled, or if user marks it as cancelled when killing it.
    if entry["done"] and entry["cancelled"] > 1 and (entry["running"] + entry["idle"] + entry["held"] +  entry["failed"] + entry["completed"]) == 0:
        return "Cancelled"
    if entry["done"]:
        return "Completed"
    if entry["held"] > 0:
        return "Held"
    if entry["running"] == 0 and entry["idle"] != 0:
        return "Idle"
    if entry["running"] > 0:
        return "Running"
    return "Unknown"

def format_poll_interval(seconds):
    # Calculate minutes and remaining seconds
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    
    # Create a friendly format string
    if minutes == 1:
        minute_string = "1 minute"
    else:
        minute_string = f"{minutes} minutes"
    
    if remaining_seconds == 1:
        second_string = "1 second"
    else:
        second_string = f"{remaining_seconds} seconds"
    
    # Combine the strings
    if minutes == 0:
        return second_string
    elif remaining_seconds == 0:
        return f"{minute_string}"
    else:
        return f"{minute_string}, {second_string}"
    
    
def calculate_next_run(poll_interval):
    # Calculate the next run time
    last_run = datetime.now(utc)
    next_run = (last_run + timedelta(seconds=poll_interval)).isoformat()
    time_until_next_run = format_poll_interval(poll_interval)
    return next_run, time_until_next_run