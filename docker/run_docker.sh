#!/bin/bash
#docker run -i -p 22:22 -p 8888:8888 -v $PWD:/peachbox -t peachstone/dev /bin/bash
docker run -i -p 8888:8888 -p 12322:22 -v $PWD:/peachbox -t peachstone/dev /bin/bash
