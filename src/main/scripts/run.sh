#!/usr/bin/env bash
#
# Launcher for simple-cassandra-load-generator.
#
# Expects the layout the dist zip unpacks to:
#   simple-cassandra-load-generator/
#     simple-cassandra-load-generator.jar   <- code only, ~40KB, cheap to redeploy
#     lib/*.jar                             <- dependencies, uploaded once
#     bin/run.sh
#
# JAVA_HOME must point at a JDK 25; the project is compiled with --release 25 and
# the JVM flags below are 25-era.
#
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$HERE/simple-cassandra-load-generator.jar"

# JAVA_HOME if set, otherwise whatever java is on PATH. No install path is baked
# in here: a deployment whose JDK is not on PATH should export JAVA_HOME.
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"

[[ -f "$JAR" ]] || { echo "ERROR: $JAR not found - run from the unpacked dist" >&2; exit 1; }
command -v "$JAVA" >/dev/null 2>&1 || { echo "ERROR: no java at '$JAVA' - set JAVA_HOME to a JDK 25" >&2; exit 1; }

major="$("$JAVA" -version 2>&1 | awk -F'"' '/version/{split($2,v,"."); print v[1]; exit}')"
if [[ "${major:-0}" -lt 25 ]]; then
    echo "ERROR: java $major found at ${JAVA_HOME:-\$PATH}; this build targets 25" >&2
    echo "       set JAVA_HOME=/path/to/jdk25" >&2
    exit 1
fi

# ZGC rather than G1: the generator's own pauses show up as request latency, and
# a pause here is indistinguishable from server latency in the numbers.
#
# -XX:+UseCompactObjectHeaders: a product flag as of 25 (no
# UnlockExperimentalVMOptions needed), 8-byte headers instead of 12.
#
# --sun-misc-unsafe-memory-access=allow: netty still uses those methods and JDK
# 24+ warns on every call site otherwise.
DEFAULT_OPTS=(
    -Xms8g -Xmx8g
    -XX:+UseZGC
    -XX:+UseCompactObjectHeaders
    -XX:+AlwaysPreTouch
    --sun-misc-unsafe-memory-access=allow
    -Dcom.sun.management.jmxremote=false
)

read -r -a JVM_OPTS <<< "${LOADGEN_JVM_OPTS:-${DEFAULT_OPTS[*]}}"

exec "$JAVA" "${JVM_OPTS[@]}" -jar "$JAR" "$@"
