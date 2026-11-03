#!/bin/sh
here=$(cd "$(dirname "$0")" && pwd)
exec python3 "$here/src/main.py" "$@"
