def optimize_partitions(df):
    record_count = df.count()  
    rows_per_file = 1_000_000
    optimal_files = (int(record_count /    rows_per_file) + int(record_count % rows_per_file > 0
        ))       
    NUMBER_OF_PARTITIONS = max(1, optimal_files)
    df = df.coalesce(NUMBER_OF_PARTITIONS)
return df  
