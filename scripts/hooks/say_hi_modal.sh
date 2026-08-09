#!/bin/zsh

# UserPromptSubmit hooks are synchronous. Detach the AppleScript process so the
# Codex turn does not wait for the user to dismiss the dialog.
/usr/bin/nohup /usr/bin/osascript \
  -e 'display dialog "Hi" with title "Warden" buttons {"OK"} default button "OK" giving up after 15' \
  </dev/null >/dev/null 2>&1 &!

exit 0
