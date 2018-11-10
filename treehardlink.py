#!/usr/bin/env python3

import os
import subprocess
import sqlite3
import sys
from shlex import quote as shell_arg_quote

conn = sqlite3.connect('treehardlink.sqlite3')

tbl = """
CREATE TABLE files (
inode INT PRIMARY KEY,
dir VARCHAR,
path VARCHAR,
mode INT NULL,
number_of_links INT NULL,
uid INT NULL,
gid INT NULL,
size INT NULL,
mtime INT NULL,
ctime INT NULL,
found INT NULL
);
"""

idxes = (
    "CREATE INDEX idx_files_dir  ON files(dir)",
    "CREATE INDEX idx_files_path  ON files(path)",
    "CREATE INDEX idx_files_number_of_links ON files(number_of_links)",
)

c = conn.cursor()
c.execute(tbl)
conn.commit()


for dir in sys.argv[1:]:
    dir = dir.strip()
    if not os.path.exists(dir):
        print("Directory not found: {}".format(dir))
        exit(1)
    print("Getting file list of {}...".format(dir))
    find_cmd = "find {} -type f".format(shell_arg_quote(dir))
    p = subprocess.Popen(find_cmd, shell=True, stdout=subprocess.PIPE)
    filenames = []
    for filename in p.stdout.readlines():
        filenames.append(filename)
    print("Statting files in dir {}...".format(dir))
    for filename in filenames:
        filename = filename.strip()
        dirlen = len(dir)
        path = filename[dirlen:]
        s = os.stat(filename)
        # TODO: look if this can be done more efficient
        sql = """INSERT OR IGNORE INTO files (inode, dir, path, mode, number_of_links, uid, gid, size, mtime, ctime, found)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """
        c.execute(sql, (s.st_ino, dir, path, s.st_mode, s.st_nlink, s.st_uid, s.st_gid, s.st_size, s.st_mtime, s.st_ctime))
        c.execute("UPDATE files SET found = found + 1 WHERE inode = ?", (s.st_ino,))
    conn.commit()

print("Applying indexes...")
for idx in idxes:
    c.execute(idx)
conn.commit()
print("Done.")


