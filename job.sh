#! /bin/bash
#$ -l h_rt=240:00:00
#$ -l h_vmem=1.5G
#$ -q research-el7.q
#$ -M kristersk@met.no
#$ -m ae
#$ -R y
#$ -S /bin/bash
#$ -v PWD
#$ -cwd

python3 get-CAMS-data.py