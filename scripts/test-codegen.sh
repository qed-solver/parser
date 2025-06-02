#!/bin/bash

# Script to generate code for each rule and test whether the rules can be applied correctly

echo "## Code Generation Test Results" >> $GITHUB_STEP_SUMMARY
echo "" >> $GITHUB_STEP_SUMMARY

# Step 1: Generate code for each rule in RRuleInstances
# Create temporary Java file for code generation
cat > RuleGenerator.java << 'EOF'
import org.qed.Generated.CalciteTester;
import org.qed.*;
import java.nio.file.*;

public class RuleGenerator {
    public static void main(String[] args) throws Exception {
        String className = args[0];
        Class<?> clazz = Class.forName(className);
        RRule rule = (RRule) clazz.getDeclaredConstructor().newInstance();
        
        CalciteTester tester = new CalciteTester();
        tester.serialize(rule, CalciteTester.genPath);
        
        System.out.println("Generated code for: " + rule.name());
    }
}
EOF

# Build classpath
MAVEN_CP=$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)
CLASSPATH="target/classes:${MAVEN_CP}"

# Compile the generator
javac -cp "$CLASSPATH" RuleGenerator.java

# Generate code for each rule
find src/main/java/org/qed/Generated/RRuleInstances -name '*.java' -not -path '*/RRuleInstances-unprovable/*' | while read file; do
    className=$(echo "$file" | sed 's|src/main/java/||; s|/|.|g; s|\.java$||')
    echo "Processing: $className"
    java -cp ".:$CLASSPATH" RuleGenerator "$className"
done

# Step 2: Run all test classes
# Store results for summary
test_results=""
total_tests=0
passed_tests=0

# Find all test files and run them
find src/main/java/org/qed/Generated/Tests -name '*Test.java' | sort | while read test_file; do
    class_name=$(echo "$test_file" | sed 's|src/main/java/||; s|/|.|g; s|\.java$||')
    test_name=$(basename "$test_file" .java)
    # Remove "Test" suffix for display
    display_name=${test_name%Test}
    
    # Run the test and capture output
    if java -cp "$CLASSPATH" "$class_name" > /tmp/test_output.txt 2>&1; then
        if grep -q "false-succeeded" /tmp/test_output.txt; then
            result="⚠️ ${display_name}: FALSE-SUCCEEDED" 
            echo "$result" >> /tmp/test_results.txt
            echo "0" >> /tmp/test_counts.txt
            cat /tmp/test_output.txt
        elif grep -q "succeeded" /tmp/test_output.txt && ! grep -q "failed" /tmp/test_output.txt; then
            result="✅ ${display_name}: PASSED" 
            echo "$result" >> /tmp/test_results.txt
            echo "1" >> /tmp/test_counts.txt
        else
            result="❌ ${display_name}: FAILED" 
            echo "$result" >> /tmp/test_results.txt
            echo "0" >> /tmp/test_counts.txt
            cat /tmp/test_output.txt
        fi
    else
        result="❌ ${display_name}: ERROR"
        echo "$result" >> /tmp/test_results.txt
        echo "0" >> /tmp/test_counts.txt
        cat /tmp/test_output.txt
    fi
done

# Calculate totals
if [ -f /tmp/test_counts.txt ]; then
    total_tests=$(wc -l < /tmp/test_counts.txt)
    passed_tests=$(grep -c "1" /tmp/test_counts.txt || echo "0")
fi

# Clean up
rm -f RuleGenerator.java RuleGenerator.class /tmp/test_output.txt

# Summary
if [ -f /tmp/test_results.txt ]; then
    cat /tmp/test_results.txt | while read line; do
        echo "$line" >> $GITHUB_STEP_SUMMARY
    done
fi
echo "" >> $GITHUB_STEP_SUMMARY
echo "**Summary:** $passed_tests/$total_tests passed" >> $GITHUB_STEP_SUMMARY

# Clean up test files
rm -f /tmp/test_results.txt /tmp/test_counts.txt

# Exit with error if tests failed
if [ "$passed_tests" -ne "$total_tests" ]; then
    exit 1
fi