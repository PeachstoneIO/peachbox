PWD=`pwd`

docker run -i -p 8888:8888 -p 12322:22 -v $PWD:/peachbox -t peachstone/dev /bin/bash
