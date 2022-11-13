#!/usr/bin/env bash

set -ev

./main --database data --script <(echo "CREATE TABLE y (year int, age int, name text)")
./main --database data --script <(echo "INSERT INTO y VALUES (2010, 38, 'Gary')")
./main --database data --script <(echo "INSERT INTO y VALUES (2021, 92, 'Teej')")
./main --database data --script <(echo "INSERT INTO y VALUES (1994, 18, 'Mel')")

# Basic query
./main --database data --script <(echo "SELECT name, age, year FROM y")

# With WHERE
./main --database data --script <(echo "SELECT name, year, age FROM y WHERE age < 40")

# With operations
./main --database data --script <(echo "SELECT 'Name: ' || name, year + 30, age FROM y WHERE age < 40")
