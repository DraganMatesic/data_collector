def ns_to_sec(nanoseconds):
    """Converts nanoseconds to seconds"""
    return nanoseconds / 1000000


def sec_to_min(seconds, round_to=0):
    """Converts seconds to minutes"""
    return int(seconds/60)


def min_to_h(minutes):
    """Converts minutes to hours"""
    return int(minutes/60)


def sec_to_h(seconds):
    """Converts seconds to hours"""
    return min_to_h(sec_to_min(seconds))

def to_none(value):
    """Converts None NaN NaT to None"""
    if value is None:
        return None
    if str(value).lower() in ['none', 'nan', 'nat']:
        return None
    return value
