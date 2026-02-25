#!/usr/bin/env bash

set -e

if ! cargo bench --help &> /dev/null; then
    echo "error: cargo bench not available"
    exit 1
fi

FILTER="${1:-}"
BASELINE="${2:-}"

if [ -n "$BASELINE" ]; then
    echo "Running benchmarks and saving baseline: $BASELINE"
    cargo bench --bench dbms -- --save-baseline "$BASELINE" $FILTER
elif [ -n "$FILTER" ]; then
    echo "Running filtered benchmarks: $FILTER"
    cargo bench --bench dbms -- $FILTER
else
    echo "Running all benchmarks..."
    cargo bench --bench dbms
fi

echo ""
echo "Benchmarks complete."
echo ""
echo "View the HTML report at: target/criterion/report/index.html"
echo ""
echo "Usage examples:"
echo "  ./bench.sh                          # Run all benchmarks"
echo "  ./bench.sh insert                   # Run only insert benchmarks"
echo "  ./bench.sh scan baseline-v1         # Run scan benchmarks and save baseline"
echo "  ./bench.sh '' baseline-v1           # Run all and save baseline"
