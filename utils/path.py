import os
import threading
import sys


# progress bar callback
# useful to display progress of the upload task.
# it leverages python closure property to hold
# the state for the callback function


def progress(filename):

    size = os.path.getsize(filename)
    seen_so_far = 0
    lock = threading.Lock()

    def worker(bytes_amount):

        nonlocal seen_so_far
        with lock:
            seen_so_far += bytes_amount
            percentage = (seen_so_far / size) * 100
            msg = "\r%s  %s / %s  (%.2f%%)" % (
                filename, seen_so_far, size, percentage
            )
            sys.stdout.write(msg)
            sys.stdout.flush()
    return worker


# hierarchical paths generator


def dir_tree(folder_name, reverse=False):

    for root, dirs, files in os.walk(folder_name, topdown=True):
        if reverse:
            # inverse lexicographic order
            files.reverse()
        for name in files:
            if name is not None and not name.endswith('.DS_Store'):
                yield os.path.join(root, name)


# similar in concept to split. the head contains
# the root, the tail everything else.


def reverse_split(path):

    res = path.split(os.path.sep, 1)

    if len(res) == 1:
        res.append('')

    return res
