#!/usr/bin/env bash
# Start one Claude Code agent per database, each in its own git worktree and tmux session.
#
#   bash /opt/sdk-test-dbs/connectors/start_porting_agents.sh exasol mimer cubrid h2
#
# For each database:
#   - creates ~/wt-<db> on branch <db>-general (from $BASE, default fix-db-seeding) if missing;
#     fast-forwards an existing worktree to $BASE only if it has no commits or changes of its own
#   - starts tmux session <db> running Claude with the prompt "Do your porting task", unless a
#     session with that name already exists
# The task itself is defined in CLAUDE.md ("Porting tasks"), so nothing depends on this session.
# Attach with `tmux attach -t <db>` (the first start in a new folder asks you to trust it).
set -euo pipefail

REPO=/opt/sdk-test-dbs/connectors
BASE=${BASE:-fix-db-seeding}
KNOWN="exasol mimer cubrid h2 geode"
PROMPT="Do your porting task: read the 'Porting tasks' section of CLAUDE.md and follow it for the database this worktree is for."

if [ $# -eq 0 ]; then
    echo "usage: $0 <db>... (any of: $KNOWN)" >&2
    exit 2
fi
command -v tmux >/dev/null || { echo "tmux not found" >&2; exit 1; }
command -v claude >/dev/null || { echo "claude not found on PATH" >&2; exit 1; }

git -C "$REPO" fetch -q origin "$BASE" || echo "warning: could not fetch origin/$BASE; using the local branch" >&2

for db in "$@"; do
    case " $KNOWN " in *" $db "*) ;; *) echo "$db: unknown database (expected one of: $KNOWN)" >&2; continue ;; esac
    wt="$HOME/wt-$db"
    branch="$db-general"

    if [ ! -d "$wt" ]; then
        if git -C "$REPO" show-ref -q --verify "refs/heads/$branch"; then
            git -C "$REPO" worktree add -q "$wt" "$branch"
        else
            git -C "$REPO" worktree add -q -b "$branch" "$wt" "$BASE"
        fi
        echo "$db: created worktree $wt on $branch"
    fi

    # Bring an untouched worktree up to date; never move one that has its own commits or edits.
    if [ -z "$(git -C "$wt" status --porcelain)" ] && [ -z "$(git -C "$wt" rev-list "$BASE..HEAD")" ]; then
        git -C "$wt" merge -q --ff-only "$BASE" && echo "$db: worktree at $(git -C "$wt" rev-parse --short HEAD) ($BASE)"
    else
        echo "$db: worktree has its own work; left as is"
    fi

    if tmux has-session -t "$db" 2>/dev/null; then
        echo "$db: tmux session already running; attach with: tmux attach -t $db"
        continue
    fi
    # Keep a shell open after Claude exits so the session (and its output) survives.
    tmux new-session -d -s "$db" -c "$wt" "claude \"$PROMPT\"; exec bash"
    echo "$db: started; attach with: tmux attach -t $db"
done
