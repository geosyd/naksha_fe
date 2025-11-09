#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

def main():
    # Setup logging first to capture all output
    from src.log import setup_logging
    setup_logging()

    from src.cli import CLI
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cli = CLI()

    try:
        args = cli.parse_arguments()
        success = cli.run_command(args)
        sys.exit(0 if success else 1)
    except SystemExit:
        return 1

if __name__ == "__main__":
    main()