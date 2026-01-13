#!/bin/bash

DIR=$(dirname "$0")

# Try to find Python 3.12 or newer first, then fall back to any python3
if hash python3.13 2> /dev/null; then
    PYTHON=python3.13
elif hash python3.12 2> /dev/null; then
    PYTHON=python3.12
elif hash python3 2> /dev/null; then
    PYTHON=python3
else
    echo 'python3 required'
    exit 1
fi

$PYTHON -m venv $DIR/.venv
if [ ! -f $DIR/.venv/bin/pip ]; then
    wget https://bootstrap.pypa.io/get-pip.py
    $DIR/.venv/bin/python get-pip.py
    rm -f get-pip.py
fi

$DIR/.venv/bin/pip install -U pip setuptools wheel
$DIR/.venv/bin/pip install -r $DIR/requirements.txt

# Only install sql-cli if Python version is 3.12+
PYTHON_VERSION=$($DIR/.venv/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -gt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 12 ]); then
    echo "Installing sql-cli with Python $PYTHON_VERSION..."
    $DIR/.venv/bin/pip install -e $DIR/sql-cli
else
    echo "Warning: Python $PYTHON_VERSION is too old for sql-cli (requires >=3.12). Skipping sql-cli installation."
    echo "Doctest will continue without sql-cli support."
fi
