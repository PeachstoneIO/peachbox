PWD=`pwd`

docker run -i -p 8888:8888 -v $PWD:/peachbox -t peachstone/dev /bin/bash
