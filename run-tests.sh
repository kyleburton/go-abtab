set -eu

if [ ! -x ./abtab ]; then
  go get github.com/kyleburton/go-eval/pkg/eval
  go build
fi

function assert_files_identical () {
  EXPECTED="$1"
  ACTUAL="$2"
  if ! diff "$EXPECTED" "$ACTUAL" >/dev/null 2>&1; then
    echo_lred "FAIL"
    echo ""
    diff -U2 "$EXPECTED" "$ACTUAL" 
    exit 1
  fi
}

function clean_all () {
  rm ./fixtures/test-output/* 2>/dev/null || echo "no fixture files to clean up."
}

function echo_color () {
  COLOR="$1"
  shift
  if [ "-n" = "$1" ]; then
    shift
    echo -n -e "\e[${COLOR}m$@\e[0m"
    return 0
  fi
  echo -e "\e[${COLOR}m$@\e[0m"
}

function echo_red () {
  echo_color "0;31" "$@"
}

function echo_lred () {
  echo_color "1;31" "$@"
}

function echo_green () {
  echo_color "0;32" "$@"
}

function echo_lgreen () {
  echo_color "1;32" "$@"
}

function echo_yellow  () {
  echo_color "1;33" "$@"
}

function echo_blue    () {
  echo_color "0;34" "$@"
}

function echo_lblue   () {
  echo_color "1;34" "$@"
}

function echo_magenta () {
  echo_color "0;35" "$@"
}

function echo_lmagenta () {
  echo_color "1;35" "$@"
}

function echo_cyan    () {
  echo_color "0;36" "$@"
}

function echo_lcyan    () {
  echo_color "1;36" "$@"
}

 
clean_all

########################################
echo ""
echo_lmagenta "abcat"

echo_lblue -n "  can specify header "
./bin/abcat -i "tab://fixtures/galbithink.org/92f10-19.tab?skipLines=4&header=Name,Count" -o "tab://fixtures/test-output/header.tab"
assert_files_identical fixtures/test-output/header.tab fixtures/expectations/92f10-19.tab
echo_lgreen "OK"

echo_lblue -n "  can skip leading lines "
./bin/abcat -i "tab://fixtures/galbithink.org/92f10-19.tab?skipLines=4&header=Name,Count" -o "tab://fixtures/test-output/skip.tab"
assert_files_identical fixtures/test-output/skip.tab fixtures/expectations/92f10-19.tab
echo_lgreen "OK"

echo_lblue -n "  handles stdin "
cat fixtures/expectations/stratsol.tab | ./bin/abcat -o "tab://fixtures/test-output/stratsol.tab"
assert_files_identical fixtures/test-output/stratsol.tab fixtures/expectations/stratsol.tab
echo_lgreen "OK"

########################################
echo ""
echo_lmagenta "abhead"

echo_lblue -n "  first 10 records "
./bin/abhead -n 10 -i tab://fixtures/expectations/stratsol.tab -o tab://fixtures/test-output/stratsol-10.tab
assert_files_identical fixtures/expectations/stratsol-10.tab fixtures/test-output/stratsol-10.tab
echo_lgreen "OK"

echo_lblue -n "  first 10 records from stdin "
cat fixtures/expectations/stratsol.tab | ./bin/abhead -n 10 > fixtures/test-output/stratsol-10.tab
assert_files_identical fixtures/expectations/stratsol-10.tab fixtures/test-output/stratsol-10.tab
echo_lgreen "OK"

########################################
echo ""
echo_lmagenta "abtail"

echo_lblue -n "  last 10 records "
cat fixtures/expectations/stratsol.tab | ./bin/abtail -n 10 > fixtures/test-output/stratsol-last-10.tab
assert_files_identical fixtures/expectations/stratsol-last-10.tab fixtures/test-output/stratsol-last-10.tab
echo_lgreen "OK"

########################################
echo ""
echo_lmagenta "abgrep"

echo_lblue -n "  'true' returns all records "
./bin/abgrep -e 'true' -i "tab://fixtures/expectations/stratsol.tab" > fixtures/test-output/stratsol.grep-true.tab
assert_files_identical fixtures/expectations/stratsol.tab fixtures/test-output/stratsol.grep-true.tab
echo_lgreen "OK"

echo_lblue -n "  'gname==\"Isabella\"' "
./bin/abgrep -e 'gname=="Isabella"' -i "tab://fixtures/expectations/stratsol.tab" > fixtures/test-output/stratsol.grep-Isabella.tab
assert_files_identical fixtures/expectations/stratsol-Isabella.tab fixtures/test-output/stratsol.grep-Isabella.tab
echo_lgreen "OK"

echo_lblue -n "  'ParseFloat(year_of_birth) >= 1600.0' "
./bin/abgrep -e 'ParseFloat(year_of_birth) >= 1600.0' -i "tab://fixtures/expectations/stratsol.tab" > fixtures/test-output/stratsol.grep-year_of_birth_gt_1600.tab
assert_files_identical fixtures/expectations/stratsol-year_of_birth_gt_1600.tab fixtures/test-output/stratsol.grep-year_of_birth_gt_1600.tab
echo_lgreen "OK"

echo_lblue -n "  'Substr(gname,0,1) == \"J\"' "
./bin/abgrep -e 'Substr(gname,0,1) == "J"' -i "tab://fixtures/expectations/stratsol.tab" > fixtures/test-output/stratsol.grep-gname-J.tab
assert_files_identical fixtures/expectations/stratsol-gname-J.tab fixtures/test-output/stratsol.grep-gname-J.tab
echo_lgreen "OK"

echo_lblue -n "  'Substr(gname,-1,0) == \"y\"' "
./bin/abgrep -e 'Substr(gname,-1,0) == "y"' -i "tab://fixtures/expectations/stratsol.tab" > fixtures/test-output/stratsol.grep-gname-last-letter-y.tab
assert_files_identical fixtures/expectations/stratsol-gname-last-letter-y.tab fixtures/test-output/stratsol.grep-gname-last-letter-y.tab
echo_lgreen "OK"

########################################
echo ""
echo_lmagenta "abcut"

echo_lblue -n "  can re-order columns "
./bin/abcut -f Count,Name -i "tab://fixtures/galbithink.org/92f10-19.tab?skipLines=4&header=Name,Count" -o "tab://fixtures/test-output/92f10-19.cut-count-name.tab"
assert_files_identical fixtures/test-output/92f10-19.cut-count-name.tab fixtures/expectations/92f10-19.cut-count-name.tab
echo_lgreen "OK"

########################################
echo ""
echo_lmagenta "abview"
echo_lblue -n "  can re-order columns "
./bin/abhead -n 2 -i fixtures/fill-rate-test-1.input.tab | ./bin/abview  > fixtures/test-output/abview-1.txt
assert_files_identical fixtures/test-output/abview-1.txt fixtures/expectations/abview-1.txt
echo_lgreen "OK"

########################################
echo ""
echo_lmagenta "abfillrates"
echo_lblue -n "  counts fill rates "
./bin/abfillrates -i fixtures/fill-rate-test-1.input.tab > fixtures/test-output/fill-rate-test-1.txt
assert_files_identical fixtures/expectations/fill-rate-test-1.txt fixtures/test-output/fill-rate-test-1.txt
echo_lgreen "OK"

########################################
echo ""
echo_lmagenta "absort"
echo_lred "NO TESTS"
# go build && ./abtab -task sort -k Name -i "tab://fixtures/galbithink.org/92f10-19.tab?skipLines=4&header=Name,Count" 
# go build && ./abtab -task sort -k Count,Name -i "tab://fixtures/galbithink.org/92f10-19.tab?skipLines=4&header=Name,Count" 

########################################
echo ""
echo_lmagenta "abmod"
echo_lred "NO TESTS"

