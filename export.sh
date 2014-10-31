#!/bin/bash
export _JAVA_OPTIONS=-Xmx60G
jar='/export/home/mraza/Rwanda_Agg_Code/target/scala-2.10/cogstats_2.10-0.1.jar'
COG_file_suffix='_MonthlyCOG'
Call_file_suffix='-Call.pai.sordate.txt'
hadoop_outdir_prefix='/user/mraza/Rwanda_Out/'
hadoop_outdir_suffix='*/part-*'

output_path='/export/home/mraza/Rwanda_Output/'
output_suffix='_MonthlyCOG'

exec_obj_name='COGStats'

for month in 0606
do
	

hadoop fs -cat hdfs:///user/mraza/Rwanda_In/9_26/CallsVolDegree/${month}CallVolDegree/part-00000>>${output_path}/0926/${month}CallVolDegree
done
