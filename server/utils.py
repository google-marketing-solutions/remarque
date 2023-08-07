import datetime

def format_duration(duration: datetime.timedelta):
    total_seconds = duration.total_seconds()
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = duration.microseconds // 1000
    if hours > 0:
      return '{:02}:{:02}:{:02}.{:03d}'.format(int(hours), int(minutes), int(seconds), milliseconds)
    return '{:02}:{:02}.{:03d}'.format(int(minutes), int(seconds), milliseconds)
