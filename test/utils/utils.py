import re
import tempfile


def squeeze(str_):
    return re.sub("\s+", " ", str_).strip()


def make_tempfile(prefix="__test__", suffix=".tmp", dir_="."):
    tf = tempfile.NamedTemporaryFile(suffix=suffix, prefix=prefix, dir=dir_)
    return tf


def tempfile_write(fileobj, txt):
    fileobj.write(txt)
    fileobj.seek(0)