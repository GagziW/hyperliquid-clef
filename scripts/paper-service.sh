#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
LABEL=com.prediction-markets.eth-paper
DOMAIN="gui/$(id -u)"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
PYTHON="$ROOT/.venv/bin/python"

case "${1:-status}" in
  start)
    test -x "$PYTHON" || { echo "Install the project environment first." >&2; exit 1; }
    mkdir -p "$HOME/Library/LaunchAgents" "$ROOT/runs/service"
    if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
      echo "Paper service is already loaded."
      exit 0
    fi
    "$PYTHON" - "$ROOT" "$PLIST" <<'PY'
import pathlib
import plistlib
import sys

root, target = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
args = [str(root / ".venv/bin/python"), "-u", "-m", "pipeline.paper", "--config", str(root / "paper.toml"),
        "--output", str(root / "runs/paper")]
if target.exists():
    existing = plistlib.loads(target.read_bytes())
    if existing.get("WorkingDirectory") != str(root):
        raise SystemExit("Existing service belongs to another directory; refusing to overwrite it.")
target.write_bytes(plistlib.dumps({
    "Label": "com.prediction-markets.eth-paper", "ProgramArguments": args, "WorkingDirectory": str(root),
    "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 30, "ExitTimeOut": 10,
    "ProcessType": "Background", "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
    "StandardOutPath": str(root / "runs/service/stdout.log"),
    "StandardErrorPath": str(root / "runs/service/stderr.log"),
}))
PY
    launchctl bootstrap "$DOMAIN" "$PLIST"
    echo "Started rules-only ETH PAPER service."
    ;;
  stop)
    if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
      launchctl bootout "$DOMAIN/$LABEL"
    fi
    echo "Paper service is stopped for this login session. The installed agent will load at next login."
    ;;
  uninstall)
    if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
      launchctl bootout "$DOMAIN/$LABEL"
    fi
    if [ -f "$PLIST" ]; then
      "$PYTHON" - "$PLIST" "$ROOT" <<'PY'
import pathlib
import plistlib
import sys
path = pathlib.Path(sys.argv[1])
if plistlib.loads(path.read_bytes()).get("WorkingDirectory") != sys.argv[2]:
    raise SystemExit("Refusing to remove an unrelated service.")
path.unlink()
PY
    fi
    echo "Paper service uninstalled; results preserved."
    ;;
  status)
    launchctl print "$DOMAIN/$LABEL" 2>/dev/null | head -25 || true
    "$PYTHON" -m pipeline.paper --output "$ROOT/runs/paper" --status
    ;;
  *) echo "Usage: $0 start|stop|status|uninstall" >&2; exit 2 ;;
esac
