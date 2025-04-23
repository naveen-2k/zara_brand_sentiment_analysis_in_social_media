%python
config_path = "hive_metastore.pipeline_metastore"
dataset_aggregation = "dataset_aggregation"
dataset_audit = "dataset_audit"
aggregation = "aggregation"
aggregation_dependency = "aggregation_dependency"
dataset = "dataset"
dataset_id = 5
yesterday=spark.sql("select current_date() - interval 1 day").collect()[0][0].strftime("%Y-%m-%d")
#yesterday = "2024-03-26"

check_audit=spark.sql(f"select * from {config_path}{dataset_audit} where dataset_id = {dataset_id} and buisness_date = '{yesterday}'").collect()
if len(check_audit) > 0:
    raise Exception("Dataset already aggregated")
else:
    aggregation_info = spark.sql(f"""
        SELECT 
            d.id AS dataset_id,
            d.name,
            d.gold_table,
            d.gold_schema,
            da.is_active,
            dsa1.status as dataset_status,
            ad.dependant_dataset_id AS dependant_dataset,
            ds.silver_table AS silver_table,  -- Assuming silver_table is a column in the dataset table
            ds.catalog,
            ds.silver_schema,
            da.aggregation_id,
            a.id AS aggregation_id,
            a.notebook_path,
            CASE 
                WHEN dsa.status = 'Validated' THEN dsa.status
                ELSE NULL
            END AS status,
            dsa.buisness_date, dsa.dataset_id AS dsa_dataset_id
            
        FROM 
            {config_path}{dataset} d
        JOIN 
            {config_path}{dataset_aggregation} da ON d.id = da.dataset_id 
        JOIN 
            {config_path}{aggregation} a ON da.aggregation_id = a.id
        JOIN 
            {config_path}{aggregation_dependency} ad ON da.dataset_id = ad.dataset_id 
        LEFT JOIN 
            {config_path}{dataset_audit} dsa ON ad.dependant_dataset_id = dsa.dataset_id 
        JOIN 
            {config_path}{dataset} ds ON ad.dependant_dataset_id = ds.id  -- Join to get silver table info
        LEFT JOIN 
            {config_path}{dataset_audit} dsa1 ON d.id = dsa1.dataset_id
        WHERE 
            da.is_active = 'yes' 
            AND d.id = {dataset_id} 
            AND dsa.buisness_date = '{yesterday}';
    """)

    aggregation_info.display()
    #aggregation_info = aggregation_info.collect()

    if aggregation_info.count() == 0:
       print("No aggregation to run")
       log_dataset_audit(dataset_id,"Success",yesterday)
       
    else:
        print("Going to aggregate")
        
        dependant_dataset_list = aggregation_info.select("dependant_dataset").collect()
        print(aggregation_info.select("silver_table").collect())
        print(f"dep list:{dependant_dataset_list}")
        dataset_id=aggregation_info.select("dataset_id").collect()[0][0]
        gold_table = aggregation_info.select("gold_table").collect()[0][0]
        gold_schema=aggregation_info.select("gold_schema").collect()[0][0]
        catalog=aggregation_info.select("catalog").collect()[0][0]

        print(aggregation_info.select("status").collect())
        result = dbutils.notebook.run(aggregation_info.select("notebook_path").collect()[0][0],
                              300,
                              arguments={
                                  "dependant_dataset_list": str(dependant_dataset_list),
                                    "silver_table": str(aggregation_info.select("silver_table").collect()),
                                    "dataset_id": str(dependant_dataset_list),
                                    "silver_schema": str(aggregation_info.select("silver_schema").collect()),
                                    "catalog": str(aggregation_info.select("catalog").collect()),
                                    "business_date":str(yesterday),
                                    "gold_table":str(gold_table),
                                    "gold_schema":str(gold_schema),
                              }
        )
        if result == "aggregated":
            print("Aggregated")
            log_dataset_audit(dataset_id,"Success",yesterday)
        else:
           log_dataset_audit(dataset_id,"Failed",yesterday)
        
