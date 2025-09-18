package org.qed.Generated;

import org.qed.RRule;
import org.qed.Generated.RRuleInstances.FilterMerge;
import org.qed.Generated.RRuleInstances.ProjectMerge;
import org.qed.Generated.RRuleInstances.JoinCommute;
import org.qed.Generated.RRuleInstances.FilterReduceFalse;
import org.qed.Generated.RRuleInstances.FilterReduceTrue; // Import the new rule

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ProxySQLTester {

    public static final String OUTPUT_PATH = "src/main/java/org/qed/Generated/proxysql";

    private int nextRuleId = 10;

    public static void main(String[] args) {
        var tester = new ProxySQLTester();

        List<RRule> rulesToGenerate = List.of(
                new FilterMerge(),
                new ProjectMerge(),
                new JoinCommute(),
                new FilterReduceFalse(),
                new FilterReduceTrue()
        );

        for (RRule rule : rulesToGenerate) {
            tester.generateRuleFile(rule);
            System.out.println();
        }
    }

    public void generateRuleFile(RRule rule) {
        int currentRuleId = this.nextRuleId;
        this.nextRuleId += 10;

        var generator = new ProxySQLGenerator();
        String ruleSql = generator.translate(currentRuleId, rule.name(), rule.before(), rule.after());

        try {
            Path outputDir = Path.of(OUTPUT_PATH);
            Files.createDirectories(outputDir);

            String fileName = rule.name() + ".sql";
            Path filePath = outputDir.resolve(fileName);
            Files.writeString(filePath, ruleSql);

        } catch (IOException | UnsupportedOperationException e) {
            e.printStackTrace();
        }
    }
}