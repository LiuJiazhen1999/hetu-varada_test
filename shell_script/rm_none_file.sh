ls | while read table
do
	path="/mydata/tpcds_parquet_300.db_rewrite_index/"$table"/"
	ls $path | while read line
	do
		if [ `grep -c "None None" $path$line` -ne '0' ];then
			rm -f $path$line
		fi
	done
done