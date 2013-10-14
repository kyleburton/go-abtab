test -d galbithink.org || mkdir galbithink.org
cd galbithink.org

if [ ! -f get-files.sh ]; then
  wget http://www.galbithink.org/names/us200.htm 
  grep '>names<' us200.htm  | perl -ane 'print "test -f $1 || wget http://www.galbithink.org/names/$1\n" if /HREF="([^"]+)"/' >> get-files.sh
fi

bash get-files.sh

function get_url() {
  URL="$1"
  F="$(basename "$URL")"
  test -f "$F" || wget "$URL"
}

get_url http://www.galbithink.org/names/rutland1296-7.txt
get_url http://www.galbithink.org/names/guild.txt
get_url http://www.galbithink.org/names/stratsol.txt
get_url http://www.galbithink.org/names/eang.txt
get_url http://www.galbithink.org/names/ncumb.txt

cd ..
