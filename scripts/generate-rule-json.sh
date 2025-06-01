#!/bin/bash

# Script to generate JSON files for all RRule instances

# Create temporary Java file for JSON generation
cat > JsonGenerator.java << 'EOF'
import org.qed.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;

public class JsonGenerator {
    public static void main(String[] args) throws Exception {
        String className = args[0];
        Class<?> clazz = Class.forName(className);
        RRule rule = (RRule) clazz.getDeclaredConstructor().newInstance();
        ObjectMapper mapper = new ObjectMapper();
        String fileName = rule.name() + "-" + rule.info() + ".json";
        ObjectNode jsonNode = rule.toJson();
        mapper.writerWithDefaultPrettyPrinter().writeValue(
            Path.of("tmp-rules", fileName).toFile(),
            jsonNode
        );
    }
}
EOF

# Build classpath
MAVEN_CP=$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)
CLASSPATH="target/classes:${MAVEN_CP}"

# Compile the generator
javac -cp "$CLASSPATH" JsonGenerator.java

# Generate JSON for each rule
find src/main/java/org/qed/Generated/RRuleInstances -name '*.java' | while read file; do
    className=$(echo "$file" | sed 's|src/main/java/||; s|/|.|g; s|\.java$||')
    echo "Generating JSON for: $className"
    java -cp ".:$CLASSPATH" JsonGenerator "$className"
done

# Cleanup
rm -f JsonGenerator.java JsonGenerator.class

echo "JSON generation complete. Files are in tmp-rules/"