#!/usr/bin/env python3
"""Generates the 2M-row CSV pair used by the speed shot in demo.tape."""

import os

here = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(here, "big_l.csv"), "w") as f:
    f.write("id,val,score,tag\n")
    for i in range(2_000_000):
        f.write(f"{i},{i * 2},{i * 0.5},t{i % 100}\n")

with open(os.path.join(here, "big_r.csv"), "w") as f:
    f.write("id,val,score,tag\n")
    for i in range(2_000_000):
        if 500_000 <= i < 500_100:
            continue  # 100 rows removed
        v = i * 2 + 1 if i % 10_000 == 0 else i * 2  # 199 rows modified
        f.write(f"{i},{v},{i * 0.5},t{i % 100}\n")

print("demo/big_l.csv and demo/big_r.csv ready (2M rows, ~54 MB each)")
