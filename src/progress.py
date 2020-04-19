import time

def human_time(time: float):
    hour_mul = 60 * 60
    minute_mul = 60
    hours = int(time / hour_mul)
    time -= hours * hour_mul

    minutes = int(time / minute_mul)
    time -= minutes * minute_mul
    seconds = int(time)

    result = f"{seconds}s"
    if minutes != 0:
        result = f"{minutes}m {result}"
    if hours != 0:
        result = f"{hours}h {result}"
    
    return result

def print_progress(progress, start_time=None):
    output = f"\rProgress: {progress*100:.2f}%"
    if start_time:
        elapsed_time = time.time() - start_time
        remaining_time = elapsed_time / progress - elapsed_time
        output = f"{output}, Elapsed: {human_time(elapsed_time)}, Remaining: {human_time(remaining_time)}\t\t"
    print(output, end='\n' if progress == 1 else '')