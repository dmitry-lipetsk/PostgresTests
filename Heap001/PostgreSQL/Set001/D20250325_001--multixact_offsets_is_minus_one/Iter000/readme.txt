Owner of this code is Postgres Professional (Russia).

Test to demonstrate a problem of database cluster migration where a multixact-offset became 4294967295 (-1).

Usage (Linux)

0) Run terminal

1) execute "source create_py_env.h"

2) define two variables:
export TEST_CFG__OLD_BIN_DIR="path_to_source_postgres"
export TEST_CFG__NEW_BIN_DIR="path_to_target_postgres"

3) Run "pytest -l -v -n 8"

-----
Log files are created in folder ./logs

Temporary files are created in folder ./tmp
