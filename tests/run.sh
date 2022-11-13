#!/usr/bin/env bash

set -e

./main --database data --script <(echo "CREATE TABLE y (year int, age int)")
./main --database data --script <(echo "INSERT INTO y VALUES (2010, 30)")
./main --database data --script <(echo "INSERT INTO y VALUES (2000, 20)")
./main --database data --script <(echo "INSERT INTO y VALUES (2000, 18)")
./main --database data --script <(echo "SELECT age, year FROM y")
