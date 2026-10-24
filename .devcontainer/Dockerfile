FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm
# The base image ships a Yarn apt repo whose signing key has expired, which makes
# every apt-get update (and so the Docker feature install) fail. We don't use Yarn.
RUN rm -f /etc/apt/sources.list.d/*yarn*
