#!/bin/bash

REPORT="test-results/BENCHMARK.md"

echo "# Compression Benchmark Analysis (80% - 99%)" > $REPORT
echo "Date: $(date)" >> $REPORT
echo "" >> $REPORT
echo "| Quality | Total Savings (%) | Avg PSNR (dB) | Space Saved | Total Size |" >> $REPORT
echo "| :--- | :--- | :--- | :--- | :--- |" >> $REPORT

echo "Starting benchmark from 80% to 99%..."

for q in {80..99}
do
    echo "Testing Quality: $q%..."
    # Run test and capture output
    OUTPUT=$(./quality-test.sh $q 2>&1)
    
    # Extract values using grep and awk from the terminal output
    SAVINGS=$(echo "$OUTPUT" | grep "Overall Savings:" | awk '{print $3}' | sed 's/%//')
    PSNR=$(echo "$OUTPUT" | grep "Average PSNR:" | awk '{print $3}')
    SAVED=$(echo "$OUTPUT" | grep "Total space saved:" | cut -d: -f2 | xargs)
    TOTAL=$(echo "$OUTPUT" | grep "Total Compressed Size:" | cut -d: -f2 | xargs)
    
    # Append to report
    echo "| $q% | $SAVINGS% | $PSNR dB | $SAVED | $TOTAL |" >> $REPORT
done

echo "" >> $REPORT
echo "## Conclusion" >> $REPORT
echo "Analysis of cost-benefit ratio (Savings vs Quality)." >> $REPORT

echo "Benchmark complete! Check $REPORT for details."
