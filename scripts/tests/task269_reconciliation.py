#!/usr/bin/env python3
"""Owned migration-014 DB, pinned native broker/current source-built PID1 IPC.

Uses the existing fixture lifecycle; no deployment or populated .env is read.
"""
from task266_provision import main

if __name__ == '__main__':
    main(startup=True)
