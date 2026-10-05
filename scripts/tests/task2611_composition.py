#!/usr/bin/env python3
"""Narrow owned DB/broker composition smoke; no deployment env or HTTP harness."""
from task266_provision import main

if __name__ == '__main__':
    main(composition=True)
