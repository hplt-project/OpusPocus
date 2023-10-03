import os
from glob import glob
from opuspocus import call, Input, Output, print_dot

lng1='uk'
lng2='en'

raw_data="data/raw"
clean_dest="data/clean"
call('mkdir', '-p', clean_dest)


for pipeline in glob(f'{raw_data}/*.filters.json'):
	prefix = os.path.basename(pipeline).replace('.filters.json', '')

	decompress = call('pigz', '-cd', Input(f'{clean_dest}/opuscleaner.{prefix}.{lng1}-{lng2}.tsv.gz'))

	decontaminate = call('decontaminate.py', '--min-length', 25,
		Input(f'data/dev/dev.{lng1}-{lng2}.tsv'),
		Input(f'data/dev/devtest.{lng1}-{lng2}.tsv'),
		stdin=decompress.stdout)

	compress = call('pigz',
		stdin=decontaminate.stdout,
		stdout=Output(f'{clean_dest}/para/{prefix}.{lng1}-{lng2}.tsv.gz'))

# Get the prefixes for "clean" labelled datasets from opuscleaner
import json
with open(f'{raw_data}/categories.json') as fh:
	clean_ds = json.load(fh).get('mapping', {}).get('clean', []);

combined = Output(f'data/train/clean.{lng1}-{lng2}.tsv.gz')
for ds_prefix in clean_ds:
	call('cat', Input(f'{clean_dest}/para/{ds_prefix}.{lng1}-{lng2}.tsv.gz'), stdout=combined)

# STEP 1 generate vocab(s)
vocab_size = 32000

sed = call('sed', 's/\t/\n/g', stdin=Input(f'data/train/clean.{lng1}-{lng2}.tsv.gz'))

generate_vocab = call('spm_train',
	'--bos_id=-1',
	'--eos_id=0',
	'--unk_id=1',
	f"--model_prefix=model.{lng1}-{lng2}",
	f'--vocab_size={vocab_size}',
	f'--input', sed.stdout,
	'--input_sentence_size=20000000',
	'--train_extremely_large_corpus',
	'--byte_fallback')

## Problem: what is the output of spm_train? How will it know that that
## code can run independently from the decontaminate step that's next?

# STEP 2.pre
# When OpusCleaner supports mono-lingual, we should do cleaning here too
# However, we also need to remove the dev/devtets from the monolingual data
# Mono is cleaned+merged earlier
for ln in [lng1, lng2]:
	decompress = call('pigz', '-dc', Input(f'data/raw/mono.{ln}.txt.gz'))

	decontaminate = call('./decontaminate.py',
		'--to-remove', 
		Input(f'data/dev/dev.{lng1}-{lng2}.tsv'),
		Input(f'data/dev/devtest.{lng1}-{lng2}.tsv'),
		stdin=decompress.stdout,
		stdout=Output(f'data/clean/mono.{ln}.txt'))


# STEP 2 Train models for iterative backtranslation
# Generate iterative translated mono text
# Note: We do not use these BT-models for the teacher because they only see
# clean parallel authentic data, and parallel from synthetic mono text
bts = 5
src = lng1

for bt_iteration in range(1, bts + 1):
	if src == lng1:
		tgt = lng2
	else:
		tgt = lng1
	
	seed=1

	inputs = [
		Input(f'data/clean/mono.{src}.txt'),
		Input(f'data/clean/mono.{tgt}.txt'),
	] if bt_iteration == 1 else [
		Input(f'data/train-{bt_iteration - 1}.{src}.txt'),
		Input(f'data/train-{bt_iteration - 1}.{tgt}.txt'),
	]

	# train lng1 -> lng2
	call('marian', '-c', 'config-bt.yml',
		'--seed', str(seed),
		'--train-sets',
			*inputs,
		'--valid-sets',
			Input(f'data/dev/dev.{src}-{tgt}.tsv'),
			# Input(f'data/dev/dev.{tgt}.txt'),
		'--model', Output(f'model-{bt_iteration}/model.npz'),
		'--num_devices', str(8))

	# # translate lng1 (mono) -> lng2 (synth i-th iteration)
	call('marian-decode',
	  '-m', Input(f'model-{bt_iteration}/model.npz'),
	  '-i', Input(f'data/clean/mono.{src}.txt'),
	  '-o', Output(f'data/synth-{bt_iteration}.{tgt}.txt'),
	  '--maxi-batch', '100', '--maxi-batch-sort', 'src',
	  '--beam-size', '6',
	  '--quiet-translation',
	  '--max-length-factor', '3',
	  '--max-length-crop',
	  '--max-length', '300',
	  '--num-devices', str(8))

	# # We must apply some light cleaning on the output of the
	# # iterative backtranslation to filter out length-ratios
	paste = call('paste',
		Input(f'data/clean/mono.{src}.txt'),
		Input(f'synth-{bt_iteration}.{tgt}.txt'))

	call('bash', 'clean-para.sh',
		stdin=paste.stdout,
		stdout=Output(f'cleaned-synth-{bt_iteration}.tsv'))

	# # prepare data (mix of auth + synth from i-th iteration)
	paste = call('paste',
		Input(f'data/train.{src}.txt'),
		Input(f'data/train.{tgt}.txt'))

	cat = call('cat',
		paste.stdout,
		Input(f'cleaned-synth-{bt_iteration}.tsv'))

	shuf = call('shuf', stdin=cat.stdout)

	cut_tgt = call('cut', '-f1', stdout=Output(f'data/train-{bt_iteration}.{tgt}.txt'))

	cut_src = call('cut', '-f2', stdout=Output(f'data/train-{bt_iteration}.{src}.txt'))

	tee = call('tee', cut_tgt.stdin, cut_src.stdin, stdin=shuf.stdout)

	# switch src and tgt, continue
	src = tgt

print_dot()