#!/bin/bash
find . -maxdepth 2 -type f -name "*.py" | xargs pylint -E