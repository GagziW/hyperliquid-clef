#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
REMOTE_ROOT=/Users/leonwitt/codeMacMini/prediction_markets/hyperliquid-clef
SSH="ssh -o BatchMode=yes -o ConnectTimeout=8 -o ForwardAgent=no"
ACTION=${1:-status}
case "$ACTION" in
  start|stop|status|uninstall)
    $SSH macmini-gagzi "sh '$REMOTE_ROOT/scripts/paper-service.sh' '$ACTION'"
    ;;
  fetch)
    mkdir -p "$ROOT/runs/macmini-paper"
    rsync -az -e "$SSH" "macmini-gagzi:$REMOTE_ROOT/runs/paper/" "$ROOT/runs/macmini-paper/"
    echo "Paper results copied to $ROOT/runs/macmini-paper"
    ;;
  *) echo "Usage: $0 start|stop|status|uninstall|fetch" >&2; exit 2 ;;
esac
