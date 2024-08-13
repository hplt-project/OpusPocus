#!/bin/bash
marian_ver="1.12.0"
marian_dir="marian_cpu"
build_dir="$marian_dir/build"

n_threads=${1:-1}

if [ -e $marian_dir ] ; then
    echo "Directory $build_dir already exists" >&2
    exit 0
fi
git clone https://github.com/marian-nmt/marian.git $marian_dir --branch $marian_ver

if [ -e $build_dir ] ; then
    echo "Directory $build_dir already exists" >&2
    exit 0
fi
mkdir -p $build_dir
cd $build_dir

cmake .. \
	-DCMAKE_BUILD_TYPE=Release \
	-DCOMPILE_CPU=ON \
	-DCOMPILE_CUDA=OFF \
	-DUSE_SENTENCEPIECE=ON
make -j $n_threads 2>&1 | tee $build_dir/build.log
