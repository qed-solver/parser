#!/bin/bash

PROXYSQL_USER="admin"
PROXYSQL_PASS="admin"
PROXYSQL_HOST="127.0.0.1"
PROXYSQL_PORT="6032"

MYSQL_USER="root"
MYSQL_PASS="wkaiz"
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="6033"

run_proxysql() {
    local sql="$1"
    docker exec -i proxysql mysql -u "$PROXYSQL_USER" -p"$PROXYSQL_PASS" -h "$PROXYSQL_HOST" -P"$PROXYSQL_PORT" -e "$sql"
}

run_mysql() {
    local sql="$1"
    echo -e "\n➡️ Running MySQL command:\n$sql\n"
    mysql -u "$MYSQL_USER" -p"$MYSQL_PASS" -h "$MYSQL_HOST" -P"$MYSQL_PORT" -e "$sql"
    echo -e "\n----------------------------------------\n"
}

for rule_file in proxysql/*.sql; do
    base_name=$(basename "$rule_file" .sql)
    test_file="Tests-ProxySQL/${base_name}Test.sql"

    echo -e "\n=============================="
    echo "📄 Processing rule file: $rule_file"
    echo "Corresponding test file: $test_file"
    echo "==============================\n"

    sql_content=$(cat <(echo "USE main;") "$rule_file")
    echo -e "➡️ Loading ProxySQL rule:\n$sql_content\n"
    run_proxysql "$sql_content"

    run_proxysql "LOAD MYSQL QUERY RULES TO RUNTIME;"
    run_proxysql "SAVE MYSQL QUERY RULES TO DISK;"

    echo -e "\n📊 ProxySQL stats after loading rule:"
    run_proxysql "SELECT * FROM stats.stats_mysql_query_rules ORDER BY hits DESC;"
    echo -e "\n----------------------------------------\n"

    test_sql=$(<"$test_file")
    run_mysql "$test_sql"

    sleep 2
    echo -e "\n📊 ProxySQL stats after running test:"
    run_proxysql "SELECT * FROM stats.stats_mysql_query_rules ORDER BY hits DESC;"
    echo -e "\n----------------------------------------\n"

    echo "🧹 Cleaning up ProxySQL rules..."
    run_proxysql "DELETE FROM mysql_query_rules;"
    run_proxysql "LOAD MYSQL QUERY RULES TO RUNTIME;"
    run_proxysql "SAVE MYSQL QUERY RULES TO DISK;"
    echo -e "\n========================================\n"
done
