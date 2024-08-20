#!/bin/bash
marian_ver="1.12.0"
marian_dir="marian_gpu"
build_dir="$marian_dir/build"

cuda_root=$1
if [ ! -e $cuda_root ]; then
	echo "$0 cuda_root_dir cudnn_version [num_threads]" >&2
	echo "cuda_root_dir ($cuda_root) does not exist" >&2
	exit 1
fi

cudnn_ver=$2
if [ -z $cudnn_ver ]; then
	echo "$0 cuda_root_dir cudnn_version [num_threads]" >&2
	echo "cudnn_version was not specified" >&2
fi

n_threads=${3:-1}

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
    -DCOMPILE_CUDA=ON \
    -DCUDA_TOOLKIT_ROOT_DIR=$cuda_root \
    -DUSE_CUDNN=ON \
    -DCUDNN_LIBRARY=$cuda_root/cudnn/$cudnn_ver/lib/libcudnn.so \
    -DCUDNN_INCLUDE_DIR=$cuda_root/cudnn/$cudnn_ver/include \
	-DUSE_SENTENCEPIECE=ON
make -j $n_threads 2>&1 | tee build.log
