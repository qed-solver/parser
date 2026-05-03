# RuleScript

RuleScript is an engine-agnostic domain-specific language (DSL) for developing query rewrite rules.
For details, please see our [paper](http://www2.eecs.berkeley.edu/Pubs/TechRpts/2024/EECS-2024-140.pdf).

## Build

The project requires Java 23. Build with Maven:

```sh
./mvnw compile -q
```

## Generate Rules

Rules are generated per backend by running the corresponding tester. First build a classpath:

```sh
./mvnw dependency:build-classpath -q -DincludeTypes=jar -Dmdep.outputFile=/tmp/cp.txt
```

Then run the tester for the target backend:

```sh
# CockroachDB
java -cp "target/classes:$(cat /tmp/cp.txt)" org.qed.Backends.Cockroach.CockroachTester

# Apache Calcite
java -cp "target/classes:$(cat /tmp/cp.txt)" org.qed.Backends.Calcite.CalciteTester

# MySQL
java -cp "target/classes:$(cat /tmp/cp.txt)" org.qed.Backends.MySQL.Tests.MySQLTester
```

Generated rule files are written to each backend's `Generated/` directory.

## Adding Rules

Rules are defined in `src/main/java/org/qed/RRuleInstances/` as Java records implementing `RRule`. Each rule provides a `before()` pattern and an `after()` transformation in terms of RuleScript's relational algebra operators. The generators pick up every file in that directory automatically.

**Example: `FilterMerge`**

```java
// src/main/java/org/qed/RRuleInstances/FilterMerge.java
public record FilterMerge() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN inner = source.pred("inner");
    static final RexRN outer = source.pred("outer");

    @Override
    public RelRN before() {
        return source.filter(inner).filter(outer);   // source.filter(P).filter(Q)
    }

    @Override
    public RelRN after() {
        return source.filter(RexRN.and(inner, outer)); // source.filter(P AND Q)
    }
}
```

Running the generators will produce:
- `src/main/java/org/qed/Backends/Calcite/Generated/FilterMerge.java` — the Apache Calcite rule implementation
- `src/main/java/org/qed/Backends/Cockroach/Generated/FilterMerge.opt` — the CockroachDB optgen rule

To also add a Calcite test, create `src/main/java/org/qed/Backends/Calcite/Tests/FilterMergeTest.java` with a `public static void runTest()` method that constructs `before` and `after` plans using `RuleBuilder` and calls `tester.verify(runner, before, after)`. The CalciteTester discovers and runs all `*Test.java` files in that directory automatically.

For a full description of the rule language and available operators, see the [paper](http://www2.eecs.berkeley.edu/Pubs/TechRpts/2024/EECS-2024-140.pdf).

## Apache DataFusion Backend

A separate Rust implementation targeting Apache DataFusion is available at [here](https://github.com/qed-solver/rulescript).

## Test Cases

### Apache Calcite

Individual rule tests live in `src/main/java/org/qed/Backends/Calcite/Tests/`. All tests are run automatically when the Calcite tester is invoked:

```sh
java -cp "target/classes:$(cat /tmp/cp.txt)" org.qed.Backends.Calcite.CalciteTester
```

### CockroachDB

The generated `.opt` rule files live in `src/main/java/org/qed/Backends/Cockroach/Generated/`.

To run them against CockroachDB:

1. Clone the [CockroachDB repository](https://github.com/cockroachdb/cockroach) and check out commit `4b80cd59c6299f26b2b4f02a96064d5127ccad94` — this is the exact state of the codebase the rules were developed against.

2. Copy the generated rule files and test data into the CockroachDB source tree:
   - Rule files → `pkg/sql/opt/norm/rules/`
   - Test data → `pkg/sql/opt/norm/testdata/rules/CockroachTests`

3. Check your environment is set up correctly:
   ```sh
   ./dev doctor
   ```

4. Build CockroachDB:
   ```sh
   ./dev build
   ```

5. Run the CockroachDB tests:
   ```sh
   ./dev test pkg/sql/opt/norm -f=TestNormRules/CockroachTests -v
   ```

## License

Copyright 2026 The Qed Team

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with
the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "
AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
