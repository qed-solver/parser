import mysql.connector
from pathlib import Path

MYSQL_USER = "root"
MYSQL_PASSWORD = "wkaiz"
MYSQL_DATABASE = "query_rewrite"
SQL_FILE = Path("C:\\Users\\wesle\\OneDrive\\Desktop\\parser\\src\\main\\java\\org\\qed\\Generated\\MySQL\\FilterMerge2.sql")  # path to your SQL file

TEST_QUERY = """
SELECT * FROM (SELECT * FROM testdb.users WHERE status = 'active') AS t0
WHERE id = 1;
"""

conn = mysql.connector.connect(
    host="localhost",
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = conn.cursor()

with SQL_FILE.open("r", encoding="utf-8") as f:
    sql_commands = f.read()

for cmd in sql_commands.split(";"):
    cmd = cmd.strip()
    if cmd:
        cursor.execute(cmd + ";")

conn.commit()
print(f"{SQL_FILE} executed successfully.")

cursor.execute("""
DELETE FROM rewrite_rules
WHERE id < (
    SELECT max_id FROM (SELECT MAX(id) AS max_id FROM rewrite_rules) AS t
);

""")
conn.commit()
print("Deleted all rules except the last one.")

cursor.execute("CALL flush_rewrite_rules();")
conn.commit()
print("Flushed rewrite rules.")

cursor.execute("SELECT * FROM rewrite_rules;")
print("Current rules in table:")
for row in cursor.fetchall():
    print(row)

cursor.execute(TEST_QUERY)
results = cursor.fetchall()

print("\nTest query results:")
for row in results:
    print(row)

cursor.execute("SHOW WARNINGS;")
warnings = cursor.fetchall()
print("\nWarnings (should indicate rewrite):")
for w in warnings:
    print(w)

cursor.close()
conn.close()
