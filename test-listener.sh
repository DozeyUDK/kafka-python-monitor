#!/bin/bash
# Szybki test: czy monitor łączy się z Kafką i czy endpointy health/metrics działają.
# Wymaga: Kafka na localhost:9092 (np. ./run-kafka.sh w kafka-demo), monitor uruchomiony w tle lub w drugim terminalu.

set -e
BASE_URL="${1:-http://localhost:8080}"

echo "=== Test listenerów (health + metrics) ==="
echo "Base URL: $BASE_URL"
echo ""

check() {
  local path="$1"
  local name="$2"
  printf "  %-8s %s ... " "$path" "$name"
  resp=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL$path" 2>/dev/null || echo "000")
  if [ "$resp" = "200" ]; then
    echo "OK ($resp)"
    return 0
  else
    echo "FAIL (HTTP $resp)"
    return 1
  fi
}

fail=0
check "/live"    "liveness"   || fail=1
check "/ready"   "readiness"  || fail=1
check "/health"  "health"     || fail=1
check "/metrics" "Prometheus" || fail=1

echo ""
if [ $fail -eq 0 ]; then
  echo "Wszystkie listenery OK."
  echo "Przykład metryk (fragment):"
  curl -s "$BASE_URL/metrics" 2>/dev/null | head -20
  exit 0
else
  echo "Niektóre endpointy nie odpowiadają. Upewnij się, że:"
  echo "  1. Kafka działa (w kafka-demo: ./run-kafka.sh)"
  echo "  2. Monitor działa: cd kafka-python-monitor && python program.py"
  exit 1
fi
