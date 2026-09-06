#!/usr/bin/env python3
"""Run the unchanged pipeline with periodic stacks for hosted capacity diagnosis."""
import faulthandler
from pathlib import Path
import runpy
import sys

ROOT = Path(__file__).resolve().parents[1]

if __name__ == '__main__':
    sys.path.insert(0, str(ROOT))
    sys.argv[0] = str(ROOT / 'pipeline.py')
    print('Diagnostic Python stacks will be recorded every five minutes; this does not stop the pipeline.', flush=True)
    faulthandler.dump_traceback_later(300, repeat=True)
    try:
        runpy.run_path(str(ROOT / 'pipeline.py'), run_name='__main__')
    finally:
        faulthandler.cancel_dump_traceback_later()
