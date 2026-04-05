#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-}"

case "$PROFILE" in
  small)
    docker compose -f docker-compose.yml -f docker-compose.small.yml up -d --build
    ;;
  medium)
    docker compose -f docker-compose.yml -f docker-compose.demo_medium.yml up -d --build
    ;;
  large)
    docker compose -f docker-compose.yml -f docker-compose.demo_large.yml up -d --build
    ;;
  *)
    cat <<'EOF'
Usage: ./run-demo-scaling.sh <small|medium|large>

Profiles:
  small        Run small profile (single instance each app service)
  medium       Run demo medium profile
  large        Run demo large profile
EOF
    exit 1
    ;;
esac
