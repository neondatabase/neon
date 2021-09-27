import os
import subprocess

from typing import Any, List


def get_self_dir() -> str:
    """ Get the path to the directory where this script lives. """
    return os.path.dirname(os.path.abspath(__file__))


def mkdir_if_needed(path: str) -> None:
    """ Create a directory if it doesn't already exist

    Note this won't try to create intermediate directories.
    """
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
    assert os.path.isdir(path)


def subprocess_capture(capture_dir: str, cmd: List[str], **kwargs: Any) -> str:
    """ Run a process and capture its output

    Output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.

    If those files already exist, we will overwrite them.
    Returns basepath for files with captured output.
    """
    assert type(cmd) is list
    base = os.path.basename(cmd[0]) + '_{}'.format(global_counter())
    basepath = os.path.join(capture_dir, base)
    stdout_filename = basepath + '.stdout'
    stderr_filename = basepath + '.stderr'

    with open(stdout_filename, 'w') as stdout_f:
        with open(stderr_filename, 'w') as stderr_f:
            print('(capturing output to "{}.stdout")'.format(base))
            subprocess.run(cmd, **kwargs, stdout=stdout_f, stderr=stderr_f)

    return basepath


_global_counter = 0


def global_counter() -> int:
    """ A really dumb global counter.

    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter
    _global_counter += 1
    return _global_counter

def debug_print(*args, **kwargs) -> None:
    """ Print to the console if TEST_DEBUG_PRINT is set in env.
    
    All parameters are passed to print().
    """
    if os.environ.get('TEST_DEBUG_PRINT') is not None:
        print(*args, **kwargs)

def lsn_to_hex(num: int) -> str:
    """ Convert lsn from int to standard hex notation. """
    return "{:X}/{:X}".format(num >> 32, num & 0xffffffff)
