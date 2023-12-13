set -x

cd build

rm -rf ./*
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=1 ..
make

if [ $? -ne 0 ]; then
    echo -e "\033[41;36m Compile failed \033[0m"
    exit
else
    echo -e "\033[41;36m Compile succeed \033[0m"
fi

echo 1 > /proc/sys/vm/drop_caches

cd ..
./bin/graphscope_bufferpool


