#!/usr/bin/env bash
#SBATCH --job-name=download-mono-en
#SBATCH --ntasks=10
#SBATCH --cpus-per-task=1
#SBATCH --partition=small
# TODO: parallelization
# TODO: include Common Crawl
set -euo pipefail
module load LUMI/22.12
module load wget

OUTDIR=$1
[[ -d $OUTDIR ]] || exit 1
cd $OUTDIR

echo "Downloading English News Crawl..." >&2
for y in `seq 7 1 22 | xargs -I{} printf "20%02d\n" {}`; do
    srun wget -c "https://data.statmt.org/news-crawl/en/news.$y.en.shuffled.deduped.gz" &
done
wait

echo "Downloading English News Discussions..." >&2
for y in `seq 11 1 19 | xargs -I{} printf "20%02d\n" {}`; do
    srun wget -c "https://data.statmt.org/news-discussions/en/news-discuss.$y.en.filtered.gz" &
done
wait

echo "Downloading English Europarl..." >&2
srun wget -c "https://www.statmt.org/europarl/v10/training-monolingual/europarl-v10.en.tsv.gz"

echo "Downloading English News Commentary..." >&2
for version in `seq 14 1 18`; do
    srun wget -c "https://data.statmt.org/news-commentary/v${version}/training-monolingual/news-commentary-v${version}.en.gz" &
done
wait
