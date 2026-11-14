#!/bin/sh
# Whippet container entrypoint.
#
# The default command is `node`. Compose runs `sim` and `bench` through the
# same entrypoint. The Iggy root password is read from a file so it never
# appears in `docker inspect` or the Compose environment block.

set -eu

if [ -n "${WHIPPET_IGGY_PASSWORD_FILE:-}" ] && [ -r "${WHIPPET_IGGY_PASSWORD_FILE}" ]; then
    WHIPPET_IGGY_PASSWORD="$(cat "${WHIPPET_IGGY_PASSWORD_FILE}")"
    export WHIPPET_IGGY_PASSWORD
elif [ -r /run/secrets/iggy_root_password ]; then
    WHIPPET_IGGY_PASSWORD="$(cat /run/secrets/iggy_root_password)"
    export WHIPPET_IGGY_PASSWORD
fi

exec /usr/local/bin/whippet "$@"
