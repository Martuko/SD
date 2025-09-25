#!/usr/bin/env bash
set -e

hostport="$1"
shift
cmd="$@"

host=$(echo $hostport | cut -d: -f1)
port=$(echo $hostport | cut -d: -f2)

echo "Esperando a $host:$port..."

while ! nc -z "$host" "$port"; do
  sleep 1
done

echo "$host:$port está disponible, ejecutando comando..."
exec $cmd
