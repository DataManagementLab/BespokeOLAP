#!/usr/bin/env bash
set -euo pipefail

pgrep -x db | while read -r pid; do
  ppid="$(ps -o ppid= -p "$pid" | tr -d ' ')"
  parent_comm="$(ps -o comm= -p "$ppid" 2>/dev/null | tr -d ' ')"

  # only root db processes (parent is not db)
  [[ "$parent_comm" == "db" ]] && continue

  # full command line (includes args)
  root_args="$(ps -p "$pid" -o args=)"

  # one-line tree, no args
  tree="$(pstree -pl "$pid" | tr -d '\n')"

  # strip the leading "db(PID)" from pstree output so we can prepend our args version
  suffix="${tree#db($pid)}"

  echo "db($pid) $root_args$suffix"
done

