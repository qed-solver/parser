import mysql.connector
from pathlib import Path

# Database connection info
MYSQL_USER = "root"
MYSQL_PASSWORD = "wkaiz"
MYSQL_DATABASE = "query_rewrite"

# Paths (relative to script.py)
RULE_DIR = Path("mysql")
TEST_DIR = Path("Test-MySQL")

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = conn.cursor()

# Iterate over all .sql files in rule directory
for rule_file in RULE_DIR.glob("*.sql"):
    # Match test file (rule_file.stem + "test.sql")
    test_file = TEST_DIR / f"{rule_file.stem}test.sql"

    if not test_file.exists():
        print(f"⚠️ No matching test file found for {rule_file.name}, skipping.")
        continue

    print(f"\n=== Running rule {rule_file.name} with test {test_file.name} ===")

    # Load and execute rule SQL
    with rule_file.open("r", encoding="utf-8") as f:
        sql_commands = f.read()

    for cmd in sql_commands.split(";"):
        cmd = cmd.strip()
        if cmd:
            try:
                cursor.execute(cmd + ";")
            except mysql.connector.Error as e:
                print(f"❌ Error executing command in {rule_file.name}: {e}")
                continue

    conn.commit()
    print(f"{rule_file} executed successfully.")

    # Delete all rules except the last one
    cursor.execute("""
    DELETE FROM rewrite_rules
    WHERE id < (
        SELECT max_id FROM (SELECT MAX(id) AS max_id FROM rewrite_rules) AS t
    );
    """)
    conn.commit()
    print("Deleted all rules except the last one.")

    # Flush rewrite rules
    cursor.execute("CALL flush_rewrite_rules();")
    conn.commit()
    print("Flushed rewrite rules.")

    # Show current rules
    cursor.execute("SELECT * FROM rewrite_rules;")
    print("Current rules in table:")
    for row in cursor.fetchall():
        print(row)

    # Load and run test query
    with test_file.open("r", encoding="utf-8") as f:
        test_query = f.read().strip()

    try:
        cursor.execute(test_query)
        results = cursor.fetchall()
        print("\nTest query results:")
        for row in results:
            print(row)
    except mysql.connector.Error as e:
        print(f"❌ Error running test query {test_file.name}: {e}")
        continue

    # Show warnings (should indicate rewrite)
    cursor.execute("SHOW WARNINGS;")
    warnings = cursor.fetchall()
    print("\nWarnings (should indicate rewrite):")
    for w in warnings:
        print(w)

# Cleanup
cursor.close()
conn.close()
