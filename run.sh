set -x

cd build

rm -rf ./*
cmake ..
make

if [ $? -ne 0 ]; then
    echo -e "\033[41;36m Compile failed \033[0m"
    exit
else
    echo -e "\033[41;36m Compile succeed \033[0m"
fi

cd ..
./bin/graphscope_bufferpool


