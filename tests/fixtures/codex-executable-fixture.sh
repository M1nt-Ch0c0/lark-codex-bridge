#!/bin/sh

fixture_name=${0##*/}

case "$fixture_name" in
  "codex app server fixture")
    if [ "$1" = "--version" ]; then
      printf '%s\n' 'codex-cli 0.146.0'
      exit 0
    fi
    [ "$1" = "app-server" ] || exit 91
    [ "$2" = "--listen" ] || exit 92
    [ "$3" = "stdio://" ] || exit 93
    while IFS= read -r line; do :; done
    exit 0
    ;;
  "codex process tree fixture")
    if [ "$1" = "--version" ]; then
      printf '%s\n' 'codex-cli 0.146.0'
      exit 0
    fi
    [ "$1" = "app-server" ] || exit 91
    [ "$2" = "--listen" ] || exit 92
    [ "$3" = "stdio://" ] || exit 93
    descendant_token="bridge-native-descendant:${0}:$$:app-server"
    /bin/sh -c 'kill -STOP "$$"' "$descendant_token" &
    child=$!
    printf '%s\t%s\n' "$child" "$descendant_token" > "${0%/*}/descendant.pid"
    exit 0
    ;;
  "codex version probe tree fixture")
    [ "$1" = "--version" ] || exit 91
    descendant_token="bridge-native-descendant:${0}:$$:version-probe"
    /bin/sh -c 'trap "" HUP; kill -STOP "$$"' "$descendant_token" &
    child=$!
    printf '%s\t%s\n' "$child" "$descendant_token" > "${0%/*}/version-descendant.pid"
    # Exit the group leader while the stopped descendant deliberately retains
    # the inherited stdout/stderr pipes. Probe cleanup must retain the zombie
    # leader identity, kill the group, reap, and prove the PGID absent.
    exit 0
    ;;
  "codex version probe detached tree fixture")
    [ "$1" = "--version" ] || exit 91
    descendant_token="bridge-native-descendant:${0}:$$:version-probe-detached"
    /bin/sh -c 'trap "" HUP; exec >/dev/null 2>&1; kill -STOP "$$"' "$descendant_token" &
    child=$!
    printf '%s\t%s\n' "$child" "$descendant_token" > "${0%/*}/version-detached-descendant.pid"
    printf '%s\n' 'codex-cli 0.149.0'
    exit 0
    ;;
  "codex supported 0.146.0") output='codex-cli 0.146.0' ;;
  "codex supported 0.149.0") output='codex-cli 0.149.0' ;;
  "codex malformed prefix") output='codex 0.146.0' ;;
  "codex unsupported 0.145.9") output='codex-cli 0.145.9' ;;
  "codex unsupported 0.147.0") output='codex-cli 0.147.0' ;;
  "codex unsupported 0.150.0") output='codex-cli 0.150.0' ;;
  "codex prerelease") output='codex-cli 0.146.0-rc.1' ;;
  "codex build metadata") output='codex-cli 0.146.0+build.1' ;;
  *) exit 90 ;;
esac

[ "$1" = "--version" ] || exit 91
printf '%s\n' "$output"
