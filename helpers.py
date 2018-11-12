import datetime


def timefilter(obj):
    now = datetime.datetime.now(datetime.timezone.utc)
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    one_day_ago = now - datetime.timedelta(hours=24)
    if one_day_ago < obj.last_modified and obj.last_modified < now:
        return True
    return False


def keyfilter(obj):
    if str(obj.key)[0:5] in list:
        return True
    return False
