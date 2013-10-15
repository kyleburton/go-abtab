go get github.com/kyleburton/go-abtab/cmd/abtab

for b in cat cut fillrates grep head sort tail view; do

  cat >$GOPATH/bin/ab$b <<HERE
#!/usr/bin/env bash

set -eu
ROOT="\$(dirname \$( cd "\$( dirname "${BASH_SOURCE[0]}" )" && pwd ))"

"$GOPATH/bin/abtab" -task $b "\$@"
HERE

  chmod 755 $GOPATH/bin/ab$b
done