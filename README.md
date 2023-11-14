# Cosette Parser

SQL parser for the [Cosette Solver](https://github.com/cosette-solver/cosette-rs)
based on that of [Apache Calcite](https://calcite.apache.org/).

## Requirements

Java version 19.

## Install

### With Nix

It is recommended to use the parser with Nix Shell:
 
```bash
nix shell github:cosette-solver/cosette-parser
```

## Usage

The parser accept list of paths to input files or folders containing input files:

``` bash
cosette-parser <path1> <path2> ...
```

### Input format

The input files should have `.sql` extension, containing a list of allowed SQL statements:

- `CREATE TABLE` statements that declare the schema of available tables
- `SELECT` statements that represent the query

The input file can interlace the declarations and queries, but the table must be defined before use.

### Output format

For each input file, the parser will dump a `.json` file as the input to `cosette-prover` and a `.rkt` file as the input to `cosette-disprover`.

## License

Copyright 2021 The Cosette Team

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with
the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "
AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
