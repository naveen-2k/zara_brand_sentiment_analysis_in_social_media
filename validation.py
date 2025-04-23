import re
import json

validation_audit="hive_metastore.pipeline_metastore.validation_audit"
dataset_audit="hive_metastore.pipeline_metastore.dataset_audit"
def extract_date_parts(date, format):
    year = re.search(r"(\d{4})", date).group(1)
    month = re.search(r"-(\d{2})-", date).group(1)
    day = re.search(r"-(\d{2})_", date).group(1)

    print("Year:", year)
    print("Month:", month)
    print("Day:", day)

    return year, month, day

def log_dataset_audit(dataset_id,status,business_date,error=""):
    exist_record=spark.sql(f"""
                           select * from {dataset_audit}
                            where 
                            dataset_id={dataset_id} 
                            and
                            buisness_date='{business_date}'
                            """).collect()
    # exist_record.show()
    if exist_record:
        print(exist_record)
        print(exist_record[0]['id'])
        print("record exist")
        sql=f"""update 
        {dataset_audit}
        set 
        status='{status}',
        error_message='{error}',
        modified_by=current_user(),
        modified_timestamp=current_timestamp()
        where
        id={exist_record[0]["id"]}
        """
        spark.sql(sql)
    else:
        sql=f"""insert into {dataset_audit}
        (dataset_id,
        status,
        error_message,
        created_by,
        created_timestamp,
        modified_by,
        modified_timestamp,
        business_date) 
        values(
            {dataset_id},
            '{status}',
            '{error}',
            current_user(),
            current_timestamp(),
            current_user(),
            current_timestamp(),
            '{business_date}'
            )"""
        spark.sql(sql)

def log_validation_audit(dcv_id,business_date,status,error=""):
    exist_record=spark.sql(f"""select * from {validation_audit}
                            where dataset_column_validation_id={dcv_id} 
                            and business_date='{business_date}'""").collect()

    # exist_record.show()
    
    if exist_record:
        print("record exist")
        sql=f"""update {validation_audit}
        set 
        status='{status}',
        error_message='{error}',
        modified_by=current_user(),
        modified_timestamp=current_timestamp() 
        where 
        id={exist_record[0]["id"]}"""
        spark.sql(sql)
    else:
        sql=f"""insert into {validation_audit}
        (dataset_column_validation_id,
        status,
        error_message,
        created_by,
        created_timestamp,
        modified_by,
        modified_timestamp,
        business_date) 

        values(
            {dcv_id},
            '{status}',
            '{error}',
            current_user(),
            current_timestamp(),
            current_user(),
            current_timestamp(),
            '{business_date}')"""
        spark.sql(sql)



metadata=spark.sql("""select 
                   a.id,a.name,a.source_name,a.catalog,a.layer,a.bronze_path,a.file_name,a.format,a.schema_information,a.file_name_format,
                   a.silver_schema,a.silver_table,a.error_table_name,
                   b.buisness_date,status 
                   from dbx_training_batch3.pipline_metastore.dataset a 
                   join dbx_training_batch3.pipline_metastore.dataset_audit b 
                   on a.id=b.dataset_id 
                   where (b.status='Success' or b.status='Failed')
                   and a.source_name='facebook' and a.layer='silver'
                   and b.created_by=current_user();""")

#display(metadata)
ingetsed_dataset=metadata.collect()
#print(ingetsed_dataset)
for data in ingetsed_dataset:
    dataset_id=data['id']
    print(f"dataset_id:{dataset_id}")
    dataset_name=data['name']
    print(f"dataset_name:{dataset_name}")
    source_name=data['source_name']
    print(f"source_name:{source_name}")
    catalog=data['catalog']
    print(f"catalog:{catalog}")
    layer=data['layer']
    print(f"layer:{layer}")
    bronze_path=data['bronze_path']
    print(f"bronze_path:{bronze_path}")
    file_name=data['file_name']
    print(f"file_name:{file_name}")
    format=data['format']
    print(f"format:{format}")
    schema_information=data['schema_information']
    print(f"schema_information:{schema_information}")
    file_name_format=data['file_name_format']
    print(f"file_name_format:{file_name_format}")
    business_date=data['buisness_date']
    business_date=str(business_date)
    print(f"e_file_name_date:{business_date}")
    status=data['status']
    print(f"status:{status}")
    error_table_name=data['error_table_name']
    print(f"error_table_name:{error_table_name}")
    silver_schema=data['silver_schema']
    print(f"silver_schema:{silver_schema}")
    silver_table=data['silver_table']
    print(f"silver_table:{silver_table}")

    year,month,day=extract_date_parts(business_date,file_name_format)
    print(f"year:{year}")
    if format=='csv':
            df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{bronze_path}/{year}/{month}/{day}/*/")
            #display(df)
            #df.show(10)
    if format=='json':
        df=spark.read.format("json").option("multiline", "false").load(f"{bronze_path}/{year}/{month}/{day}/*/")
        #display(df)
        #df.show(5)
    
    
    print(df.show(10))   
    print(f"dataset_id:{dataset_id}")
    while True:
        validation_df=spark.sql(f"""
                    SELECT 
                    dc.dataset_id as dataset_id,dsv.id as dcv_id,dsv.is_active,dsv.validation_parameters,
                    dc.id as dc_id ,dc.column_name,dc.column_datatype,
                    v.id as v_id,v.name,v.notebook_path,
                    va.status,va.business_date
                    FROM 
                    dbx_training_batch3.pipline_metastore.dataset_column_validation dsv
                    JOIN 
                    dbx_training_batch3.pipline_metastore.dataset_column dc 
                    ON dsv.dataset_column_id = dc.id
                    JOIN 
                    dbx_training_batch3.pipline_metastore.validation v 
                    ON dsv.validation_id = v.id
                    LEFT JOIN 
                    dbx_training_batch3.pipline_metastore.validation_audit va 
                    ON dsv.id = va.dataset_column_validation_id 
                    and va.business_date='{business_date}'
                    where dc.dataset_id={dataset_id}
                    and dsv.is_active='yes'
                    and (va.status IS NULL or va.status='Failed')                  
                    """)
        print(validation_df.show())
        display(validation_df)
        print(f"validadatadtion started for {dataset_id}: for business date:{business_date} ")
       
        if validation_df.count()==0:
            try:
                #put that validated and error view in silver table
                #log_dataset_audit(dataset_id,'validation passed',business_date) #log dataset audit as success
                
                error_data=spark.sql(f"""select distinct * from {catalog}.{silver_schema}.{error_table_name} where file_date='{business_date}'""")
                print(error_data.show())
                pk="id"
                
                validated_df = df.join(error_data, on=f"{pk}", how="leftanti")
                

                print("showing valid data")
                print(validated_df.show())
                validated_df.display()
                validated_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{silver_schema}.{silver_table}")
                
                log_dataset_audit(dataset_id,'Validated',business_date)
                print("validated")
                break
            except Exception as e:
                print("error")
                log_dataset_audit(dataset_id,'Validation Failed',business_date,error=str(e))

        else:
            for row in validation_df.collect():
                try:
                
                    print(f"validation in progress for {dataset_id}: for business date:{business_date} ")
                    print(f"validation column started for {row['dcv_id']} and {row['column_name']} and {row['column_datatype']} and {row['name']} and {row['notebook_path']} and {row['validation_parameters']}")
                    print("in progress")
                    result = dbutils.notebook.run(
                        row['notebook_path'],
                        timeout_seconds = 300,
                        arguments = {
                            # 'dataframe':df
                            'dcv_id':str(row['dcv_id']),
                            'column_name':row['column_name'],
                            'column_datatype':row['column_datatype'],
                            "dataset_id": str(dataset_id),
                            "business_date": business_date,
                            "bronze_path": bronze_path,
                            "year": year,
                            "month": month,
                            "day": day,
                            "format": format,
                            "dataset_name":dataset_name,
                            "error_table_name":error_table_name,
                            "silver_schema":silver_schema,
                            "catalog":catalog,
                            "validation_rule":json.dumps(row['validation_parameters'])
                            })
                    print(f"result:{result}")
                    if result=="Validation Passed" or result=="Validation Passed with no errors":
                        print(f"validation success for {row['dcv_id']} and {row['column_name']} and {row['column_datatype']} and {row['name']} and {row['notebook_path']} and {row['status']} and {row['business_date']}")
                        log_validation_audit(row['dcv_id'],business_date,'Validation Passed')
                    else:
                        print(f"validation failed for {row['dcv_id']} and {row['column_name']} and {row['column_datatype']} and {row['name']} and {row['notebook_path']} and {row['status']} and {row['business_date']} ")
                        log_validation_audit(row['dcv_id'],business_date,'Failed',error=str(result))
                except Exception as e:
                    print(e)
                    log_validation_audit(row['dcv_id'],business_date,'Failed',error=str(e))
    



