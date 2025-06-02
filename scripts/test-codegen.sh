#!/bin/bash

# Script to generate code for each rule and run all Calcite tests

set -e

# Step 1: Generate code for each rule in RRuleInstances
echo "## Generating code for each rule..."

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
echo ""
echo "## Running Calcite tests..."
echo ""

total_tests=0
passed_tests=0
failed_tests=""

# Find all test files and run them
find src/main/java/org/qed/Generated/Tests -name '*Test.java' | while read test_file; do
    class_name=$(echo "$test_file" | sed 's|src/main/java/||; s|/|.|g; s|\.java$||')
    test_name=$(basename "$test_file" .java)
    total_tests=$((total_tests + 1))
    
    echo -n "Running $test_name... "
    
    # Run the test and capture output
    if java -cp "$CLASSPATH" "$class_name" > /tmp/test_output.txt 2>&1; then
        if grep -q "false-succeeded" /tmp/test_output.txt; then
            echo "⚠️  FALSE-SUCCEEDED (rule didn't transform)"
            failed_tests="$failed_tests$test_name,"
            cat /tmp/test_output.txt
        elif grep -q "succeeded" /tmp/test_output.txt && ! grep -q "failed" /tmp/test_output.txt; then
            echo "✅ PASSED"
            passed_tests=$((passed_tests + 1))
        else
            echo "❌ FAILED"
            failed_tests="$failed_tests$test_name,"
            cat /tmp/test_output.txt
        fi
    else
        echo "❌ ERROR"
        failed_tests="$failed_tests$test_name,"
        cat /tmp/test_output.txt
    fi
    echo ""
done

# Clean up
rm -f RuleGenerator.java RuleGenerator.class /tmp/test_output.txt

# Summary
echo "## Summary"
echo "Code generation complete for all rules in RRuleInstances"
echo ""
echo "Test Results:"
echo "Total: $total_tests"
echo "Passed: $passed_tests"
echo "Failed: $((total_tests - passed_tests))"

if [ -n "$failed_tests" ]; then
    echo ""
    echo "Failed tests: ${failed_tests%,}"
    exit 1
fi