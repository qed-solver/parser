#!/bin/bash

# Test all generated rules with qed-prover

echo "## QED Prover Test Results" >> $GITHUB_STEP_SUMMARY
echo "" >> $GITHUB_STEP_SUMMARY

total_count=0
passed_count=0

for json_file in tmp-rules/*.json; do
    rule_name=$(basename "$json_file" .json)
    total_count=$((total_count + 1))
    ./qed-prover/target/release/qed-prover "$json_file" || true
    
    result_file="${json_file%.json}.result"
    if [ -f "$result_file" ] && jq -e '.provable == true' "$result_file" > /dev/null 2>&1; then
        echo "✅ $rule_name: PASSED" >> $GITHUB_STEP_SUMMARY
        passed_count=$((passed_count + 1))
    else
        echo "❌ $rule_name: FAILED" >> $GITHUB_STEP_SUMMARY
    fi
done

echo "" >> $GITHUB_STEP_SUMMARY
echo "**Summary:** $passed_count/$total_count passed" >> $GITHUB_STEP_SUMMARY