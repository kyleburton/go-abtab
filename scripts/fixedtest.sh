set -eu
# set -x
PROJ_ROOT="$(dirname $0)/.."
FIXED_URL="fixed:///dev/stdout?fields=F1:0-10,F2:11-20,F3:21-40,F4:41-60,F5:61-80&padding=~"
cd $PROJ_ROOT
bash local-build.sh

if [ -t 1 ]; then
  echo "IS TERM"
else
  echo "NOT: IS TERM"
fi
./abtab -task cat -i fixtures/fill-rate-test-1.input.tab -o "$FIXED_URL" | \
  ./abtab -task cat -i "fixed:///dev/stdin?fields=F1:0-10,F2:11-20,F3:21-40,F4:41-60,F5:61-80&padding=~" | abtab -task view
