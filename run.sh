#!/bin/bash
export _JAVA_OPTIONS=-Xmx60G
jar='/export/home/mraza/Rwanda_CallsDegreeAnalysis/target/scala-2.10/callsvoldegreestats_2.10-0.1.jar'
COG_file_suffix='_MonthlyCOG'
Call_file_suffix='-Call.pai.sordate.txt'
hadoop_outdir_prefix='/user/mraza/Rwanda_Out/'
hadoop_outdir_suffix='*/part-*'

call_file_path='/CallsFiles/'

output_path='/export/home/mraza/Rwanda_Output/'
output_suffix='_MonthlyCOG'

exec_obj_name='CallsVolDegreeStatsMain'

#for month1 in 0601 0602 0603 0604 0605 0606 0607 0608 0608 0609 0610
for month1 in 0605 #0602 0603 0604 0605 0606 0607 0608 0608 0609 0610
do
	

	spark-submit --class $exec_obj_name --master yarn-client $jar $month1 $month1$COG_file_suffix $call_file_path$month1$Call_file_suffix;

done
