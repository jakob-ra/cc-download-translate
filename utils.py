from botocore.exceptions import ClientError
import math

def exponential_backoff(func, *args, **kwargs):
    """Exponential backoff to deal with request limits"""
    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 4  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            return func(*args, **kwargs)
        except ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        raise

def convert_file_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])