# Qed Parser

SQL parser for the [Qed Solver](https://github.com/qed-solver/prover)
based on that of [Apache Calcite](https://calcite.apache.org).

## Requirements

Java version 19.

## Install

### With Nix

It is recommended to use the parser with Nix Shell:
 
```bash
nix shell github:qed-solver/parser
```

## Usage

The parser accept list of paths to input files or folders containing input files:

``` bash
qed-parser <path1> <path2> ...
```

### Input format

The input files should have `.sql` extension, containing a list of allowed SQL statements:

- `CREATE TABLE` statements that declare the schema of available tables. The statements should be valid in `MySQL`. Foreign key is not yet supported. Example:

```sql
CREATE TABLE foo (
       x INTEGER,
       y VARCHAR NOT NULL,
       PRIMARY KEY (x)
);
CREATE TABLE bar (
       z FLOAT,
       UNIQUE (z)
);
```

- `DECLARE (SCALAR|AGGREGATE) FUNCTION` statements that declares the signature of custom functions. Example:

```sql
DECLARE SCALAR FUNCTION my_add(INTEGER, INTEGER) RETURNS INTEGER;
DECLARE AGGREGATE FUNCTION my_sum(INTEGER) RETURNS INTEGER;
```

- `SELECT` statements that represent the query. The statements should be valid SQL `SELECT` statement.

The input file can interlace the declarations and queries, but tables and custom functions must be defined before use. Statements are not case-sensitive, but they must end with a semicolon.

The prover only accepts two queries in one single file for equivalence checking.

The `example.sql` and its correponding outputs may be helpful to understand how this works.

### Output format

For each input file, the parser will dump a `.json` file as the input to `qed-prover` and a `.rkt` file as the input to `qed-disprover`.

## License

Copyright 2021 The Qed Team

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with
the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "
AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
