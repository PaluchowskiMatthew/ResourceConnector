Example cluster usage:
python wrapper.py --script-command "srun -n 4 --account proj39 bbic_stack.py /your/output/path/output.h5 --create-from /your/input/path/list.txt --orientation coronal --all-stacks"


localhost test:
python wrapper.py --script-command "python tests/dummy_script.py"
