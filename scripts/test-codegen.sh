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
    java -cp ".:$CLASSPATH" RuleGenerator "$className"
done

# Step 2: Run all test classes
# Store results for summary
total_tests=0
passed_tests=0

# Find all test files and run them
find src/main/java/org/qed/Generated/Tests -name '*Test.java' | sort | while read test_file; do
    class_name=$(echo "$test_file" | sed 's|src/main/java/||; s|/|.|g; s|\.java$||')
    test_name=$(basename "$test_file" .java)
    # Remove "Test" suffix for display
    display_name=${test_name%Test}
    total_tests=$((total_tests + 1))
    
    # Run the test and capture output
    if java -cp "$CLASSPATH" "$class_name" > /tmp/test_output.txt 2>&1; then
        if grep -q "false-succeeded" /tmp/test_output.txt; then
            echo "⚠️ ${display_name}: FALSE-SUCCEEDED" >> $GITHUB_STEP_SUMMARY
        elif grep -q "succeeded" /tmp/test_output.txt && ! grep -q "failed" /tmp/test_output.txt; then
            echo "✅ ${display_name}: PASSED" >> $GITHUB_STEP_SUMMARY
            passed_count=$((passed_count + 1))
        else 
            echo "❌ ${display_name}: FAILED" >> $GITHUB_STEP_SUMMARY
        fi
    else
        echo "❌ ${display_name}: ERROR" >> $GITHUB_STEP_SUMMARY
    fi
done

# Clean up
rm -f RuleGenerator.java RuleGenerator.class /tmp/test_output.txt

echo "" >> $GITHUB_STEP_SUMMARY
echo "**Summary:** $passed_tests/$total_tests passed" >> $GITHUB_STEP_SUMMARY

# Exit with error if tests failed
if [ "$passed_tests" -ne "$total_tests" ]; then
    exit 1
fi