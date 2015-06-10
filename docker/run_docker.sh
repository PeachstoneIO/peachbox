<<<<<<< HEAD
#!/bin/bash
#docker run -i -p 22:22 -p 8888:8888 -v $PWD:/peachbox -t peachstone/dev /bin/bash
=======
PWD=`pwd`

>>>>>>> 1d0a4a3e4fabaa6c59ce9c9d5c0ebc87babd3f30
docker run -i -p 8888:8888 -p 12322:22 -v $PWD:/peachbox -t peachstone/dev /bin/bash
