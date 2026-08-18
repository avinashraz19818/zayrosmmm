#!/usr/bin/env bash
# Ubuntu's libpulsecommon is installed in a nested directory by the Apt
# buildpack, but the dynamic loader does not search that directory by default.
# This is needed by ffmpeg/ffprobe on Heroku.
export LD_LIBRARY_PATH="${HOME}/.apt/usr/lib/x86_64-linux-gnu/pulseaudio:${HOME}/.apt/usr/lib/x86_64-linux-gnu:${HOME}/.apt/usr/lib:${LD_LIBRARY_PATH:-}"
