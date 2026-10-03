# DESIGN: flink-faker for Apache Flink 2.2

Status: APPROVED by owner (Gate A, 2026-10-03) · Owner: tkaymak · Date: 2026-10-03

This document is the single source of truth for this change. If something is not specified here, do not invent it; stop and report the open question instead.

## 1. Background
- `flink-faker` is an Apache Flink table source (`'connector' = 'faker'`) that generates fake rows from [Datafaker](https://www.datafaker.net) expressions. The upstream project is `knaufk/flink-faker`; its last release was 0.5.3 for Flink 1.17 (2023).
- This repository is the fork `tkaymak/flink-faker`. The branch `flink-2.2` already contains a cherry-picked community port to Flink **2.1.0** (commit "Flink 2.1 (#1)" by truelzsch).
  - That commit changed `pom.xml`, `FakerUtils.java`, `FlinkFakerGenerator.java` and the two test classes.
  - It set the project version to `0.6.0`, `flink.version` 2.1.0, `datafaker.version` 2.4.4 and `java.version` 21.
- Goal: a jar that works on **Flink 2.2.1** and is used by the Docker playground `tkaymak/sql-cli-for-apache-flink-docker`. It is also used by students in a university course (CAS Data Engineering, FHNW).

## 2. Requirements
| # | Requirement |
|---|---|
| R1 | `flink.version` = **2.2.1**. Only change the property value; the existing dependencies (`flink-table-api-java-bridge`, `flink-streaming-java`, `flink-table-test-utils`, …) all use `${flink.version}`. Do not add new dependencies. |
| R2 | Java bytecode target **17** (`java.version` = 17). Reason: the playground runs on `flink:2.2.1-scala_2.12-java17`; class files compiled for 21 would not load on Java 17. |
| R3 | Project version stays **0.6.0**. The artifact is `target/flink-faker-0.6.0.jar`. |
| R4 | The shaded/fat jar layout of the existing build stays as it is. Do not change plugins other than what R1/R2 require. The jar must contain the `org.apache.flink.table.factories.Factory` service file that registers `com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory`. |
| R5 | `mvn -B verify` passes: all existing tests green, and the spotless check passes. |
| R6 | If Flink 2.2 removed or deprecated an API used in `src/main/java`, migrate it to the Flink 2.2 equivalent and keep the behaviour unchanged. Do not delete features or tests to make the build pass. |
| R7 | README, exactly two edits, no other README changes: (a) append these two rows at the end of the table under `## Compatibility Matrix` (they already match the existing column widths): `| 2.1           | 0.6.0 (fork)        |` and `| 2.2           | 0.6.0 (fork)        |`. Leave the gaps (1.18–2.0) as they are. (b) Insert the block below directly **before** the line `## Package`. |

README block for R7(b), verbatim:
```markdown
## About this fork

This fork ([tkaymak/flink-faker](https://github.com/tkaymak/flink-faker)) keeps flink-faker running on **Apache Flink 2.2**.
All credit for flink-faker goes to its original author [Konstantin Knauf](https://github.com/knaufk);
the Flink 2.1 port was contributed by [truelzsch](https://github.com/truelzsch/flink-faker).
It is used in the Flink SQL playground of the FHNW CAS Data Engineering course
([tkaymak/sql-cli-for-apache-flink-docker](https://github.com/tkaymak/sql-cli-for-apache-flink-docker)).
Prebuilt jars are available on the [Releases](https://github.com/tkaymak/flink-faker/releases) page of this fork.
```

Clarifications (decided by the elephant):
- Leave the unused `scala.binary.version` property untouched.
- Change only `java.version` (source/target come from it); do **not** add a `<release>` flag. The build always runs on JDK 17 (section 3), so linkage against newer JDK APIs is impossible.
- Facts already verified: `org.apache.flink:*:2.2.1` artifacts (no Scala suffix for the table modules) exist on Maven Central, and the Docker image `flink:2.2.1-scala_2.12-java17` exists (it is the official tag name for 2.2.1 even though Flink 2.x dropped the Scala APIs).
- Verified API fact: in Flink 2.2.1, `org.apache.flink.table.functions.LookupFunction` still declares `public final void eval(Object...)` and `abstract Collection<RowData> lookup(RowData)`, and `TableFunction` still declares `public final void setCollector(Collector<T>)`. `FlinkFakerLookupFunctionTest` therefore compiles as it is.
- Discovery is expected: this document cannot list every Flink API difference or the exact Flink 2.2.1 `explain()` output. Find them by running the build, and fix them under R6. This is not "inventing".
- Version: `0.6.0` was never released or published by anyone (the 2.1 port only exists as a commit), so there is no collision. The 0.6.0 release will be the Flink 2.2 / Java 17 build.
- Compile errors and test failures: R6 explicitly authorises you to fix any compile error or test failure in `src/main` or `src/test` (including `TestUtils.java`) that is caused by Flink API or output-format differences between Flink 1.17/2.1 and 2.2.1. This includes the plan-string assertion in `FlinkFakerIntegrationTest.testLimitPushDown`: adapt the expected string to Flink 2.2.1's `explain()` output while still asserting that `limit=[10]` is pushed into the faker table source scan. Keep what each test asserts. If a test fails for a reason that is clearly not an API/format difference (e.g. random-data flakiness, logic bug), stop and report it.
- `.mvn/jvm.config` already exists (5 `--add-exports` on one line, without `javac.code`). **Proactively** rewrite it to contain the six options `--add-exports jdk.compiler/com.sun.tools.javac.{api,code,file,parser,tree,util}=ALL-UNNAMED` (expanded), one per line (six lines). This is allowed in addition to section 4.
- README formatting for R7: copy the two table rows **verbatim** as given, without re-aligning. Insert the "About this fork" block so that there is exactly one blank line before it (after the "inspired by voluble" line) and one blank line between its last line and `## Package`.
- The maven-shade-plugin configuration stays as is. The reviewer checks the service file (acceptance 2).

## 3. Build environment
Nobody needs Maven or Java installed locally. Build with Docker from the repository root:
```bash
docker run --rm -v "$PWD":/src -v "$HOME/.m2":/root/.m2 -w /src maven:3-eclipse-temurin-17 mvn -B verify
```
If spotless reports formatting violations in files you changed, run `mvn -B spotless:apply` the same way, then re-run `verify`.

## 4. Files you may change
- `pom.xml`
- `src/main/java/com/github/knaufk/flink/faker/*.java` (only if R6 requires it)
- `src/test/java/com/github/knaufk/flink/faker/*.java` (only to adapt to API changes, never to weaken assertions)
- `README.md` (only R7)

- `.mvn/jvm.config` (see section 2)

Do **not** run any git commands, not even read-only ones; the reviewer handles git. Do not create files outside this repository.

Items 4 and 5 below are checked by the reviewer only (with git and with the separate playground repository); you cannot and need not check them. `mvn` writes only into `target/` (which is git-ignored), so running the build does not violate section 4.

## 5. Acceptance (the reviewer will check; as implementer you check 1–3, while 4 and 5 are the reviewer's job)
1. `mvn -B verify` in the container above exits 0; the surefire reports show 0 failures and 0 errors.
2. `target/flink-faker-0.6.0.jar` exists. `unzip -p target/flink-faker-0.6.0.jar META-INF/services/org.apache.flink.table.factories.Factory` prints `com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory`.
3. `javap -v` of `com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory` from the jar shows `major version: 61` (Java 17). For checks 2 and 3 use `docker run --rm -v "$PWD":/src -w /src eclipse-temurin:17-jdk bash -c 'jar xf target/flink-faker-0.6.0.jar META-INF/services/org.apache.flink.table.factories.Factory && cat META-INF/services/org.apache.flink.table.factories.Factory && javap -v -cp target/flink-faker-0.6.0.jar com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory | grep major; rm -rf META-INF'`.
4. `git diff` touches only the files listed in section 4.
5. Later, an end-to-end check in the playground: `CREATE TABLE t (name STRING) WITH ('connector'='faker','fields.name.expression'='#{Name.fullName}'); SELECT * FROM t LIMIT 3;` returns 3 rows on Flink 2.2.1.

## 6. Out of scope
Publishing (GitHub release), CI workflow changes, new features, and the Flink 2.3 bump.
