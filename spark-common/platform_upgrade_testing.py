import boto.ses
try:
    from dsmsca_pyspark_common import utils
except ImportError:
    import utils
try:
    from dsmsca_pyspark_common.job_context import JobContext
except ImportError:
    from job_context import JobContext
from pyspark.sql.functions import *
import sys
import json

def send_email(job_context, final_df,cluster_name,email_id):
    try:
        final_val = final_df.collect()
        final_df.show(30, False)
        header_cols = final_df.schema.names
        subject = cluster_name + " : Upgrade platform testing "
        content = "<html><body>Upgrade platform testing <br><br>" \
                  "<table  width=100% height=50% border=1><tr>"
        for column in header_cols:
            content = content + "<td  bgcolor=lightgrey>" + column + "</td>"
        content = content + "</tr>"
        format = 'html'
        for records in final_val:
            content = content + "<tr>"
            for columns in header_cols:
                content = content + "<td bgcolor=white>" + str(records[columns]) + "</td>"
        content = content + "</tr></table></body></html>"
        connection = boto.ses.connect_to_region('us-east-1')
        connection.send_email(
            email_id,
            subject,
            None,
            email_id,
            format=format,
            text_body='text',
            html_body=content
        )
    except Exception as e:
        print("Exception in Sending emails", e)

def main(args_dict):
    """
    :param args_dict: ETL args
    :return: None
    """
    with JobContext("upgrade_platform_test", params_dict['environment'], params_dict['results_date']) as job_context:
        cluster_name = params_dict.get('cluster_name')
        results_date = params_dict.get('results_date')
        prod_database = params_dict.get('prod_database')
        dev_database = params_dict.get('dev_database')
        table_name = params_dict.get('table_name')
        where_check = params_dict.get('where_check')
        s3_write = params_dict.get('s3_write')
        email_id = params_dict.get('email_id')
        print("cluster_name: ", cluster_name)
        print("results_date: ", results_date)
        print("prod_database: ", prod_database)
        print("dev_database: ", dev_database)
        print("table_name: ", table_name)
        print("where_check: ", where_check)
        print("s3_write: ", s3_write)
        print("email_id: ", email_id)
        clustername_prod = "prod-" + cluster_name
        clustername_dev = "dev-" + cluster_name
        if len(where_check) !=0:
           prod_cnt_df = job_context.spark.sql("""
                            select "{0}" as cluster_details_name ,
                            count(*) as prod_number_of_records from 
                            {1}.{2}
                            where {3} """.format(clustername_prod,prod_database,table_name,where_check))
           dev_cnt_df = job_context.spark.sql("""
                            select "{0}" as cluster_details_name ,
                            count(*) dev_number_of_records from
                            {1}.{2}
                            where {3}""".format(clustername_dev,dev_database,table_name,where_check))
        else:
           prod_cnt_df = job_context.spark.sql("""
                            select "{0}" as cluster_details_name ,
                            count(*) prod_number_of_records from
                            {1}.{2} """.format(clustername_prod,prod_database,table_name))
           dev_cnt_df = job_context.spark.sql("""
                            select "{0}" as cluster_details_name ,
                            count(*) dev_number_of_records from
                            {1}.{2} """.format(clustername_dev,dev_database,table_name))

        prod_cnt_df.createOrReplaceTempView('prod_cnt_df')
        dev_cnt_df.createOrReplaceTempView('dev_cnt_df')
        
        prod_query = """
            SELECT b.cluster_details_name, 
                   metricdataresults_label, 
                   prod_max_metric_value,
                   prod_number_of_records
            FROM  (SELECT cluster_details_name, 
                          metricdataresults_label, 
                          metricdataresults_values, 
                          Max(Cast(Trim(metricdataresults_values_explode) AS INT)) 
                          prod_max_metric_value 
                   FROM   (SELECT cluster_details_name, 
                                  metricdataresults_label, 
                                  metricdataresults_values, 
                                  Explode(metricdataresults_values)
                                  AS metricdataresults_values_explode 
                           FROM   prod_df ) a 
                   GROUP  BY 1,2,3)b
                   left join prod_cnt_df c 
                   on b.cluster_details_name = c.cluster_details_name
            """.format(clustername_prod, results_date)

        # prod data dataframe
        #prod_df = job_context.spark.read.csv("s3://nike-dsm-planning-dev/IP_Test/upgrade_platform_test/prod_log.csv",header=True)
        prod_df = job_context.spark.sql("select * from nikebi_cost.cw_emr_metrics where  cluster_details_name = '{0}' and results_date = '{1}' ".format(clustername_prod,results_date))
        prod_df.createOrReplaceTempView('prod_df')
        final_prod_df = job_context.spark.sql(prod_query)
        print("PROD Metrics")
        final_prod_df.show()
        final_prod_df.createOrReplaceTempView('final_prod_df')

        #dev dataframe
        #dev_df = job_context.spark.read.csv("s3://nike-dsm-planning-dev/IP_Test/upgrade_platform_test/dev_log.csv",header=True)
        dev_df = job_context.spark.sql("select * from nikebi_cost.cw_emr_metrics where  cluster_details_name = '{0}' and results_date = '{1}' ".format(clustername_dev,results_date))
        dev_df.createOrReplaceTempView('dev_df')
        dev_query = """
            SELECT b.cluster_details_name, 
                   metricdataresults_label, 
                   dev_max_metric_value,
                   dev_number_of_records
            FROM  (SELECT cluster_details_name, 
                          metricdataresults_label, 
                          metricdataresults_values, 
                          Max(Cast(Trim(metricdataresults_values_explode) AS INT)) 
                          dev_max_metric_value
                   FROM   (SELECT cluster_details_name, 
                                  metricdataresults_label, 
                                  metricdataresults_values, 
                                  Explode(metricdataresults_values) 
                                  AS metricdataresults_values_explode 
                           FROM   dev_df ) a 
                   GROUP  BY 1,2,3)b 
                   left join dev_cnt_df c 
                   on b.cluster_details_name = c.cluster_details_name
            """


        final_dev_df = job_context.spark.sql(dev_query)
        print("Dev Metrics")
        final_dev_df.show()
        final_dev_df.createOrReplaceTempView('final_dev_df')

        # final dataframe prod and dev values
        final_df = job_context.spark.sql("""
                    SELECT c.metricdataresults_label, 
                            c.prod_max_metric_value,
                            c.prod_number_of_records,
                            c.dev_max_metric_value,
                            c.dev_number_of_records,
                            (c.prod_max_metric_value-c.dev_max_metric_value) as variance ,
                            ((c.prod_max_metric_value-c.dev_max_metric_value)/c.prod_max_metric_value)*100 as percentage_variance
                    FROM   (SELECT a.metricdataresults_label as metricdataresults_label,  prod_max_metric_value, prod_number_of_records, dev_max_metric_value,   dev_number_of_records 
                            FROM   final_prod_df a
                            left outer join 
                            final_dev_df b
                            on a.metricdataresults_label=b.metricdataresults_label)c
                    GROUP BY 1,2,3,4,5
        """)
        print("Final values")
        final_df.show()
        final_df.write.mode("overwrite").save(s3_write)
        # sending email
        send_email(job_context, final_df,cluster_name,email_id)
if __name__ == "__main__":
    params = ' '.join(sys.argv[1:])
    print(params)
    # For Testing use the args below
    
    argv = {"environment": "dev",
        "cluster_name": "IP-BTFD-AGG",
        "results_date": "2020-04-22",
        "dev_database" : "dsmsca_dev_processed",
        "prod_database":"dsmsca_processed",
        "table_name":"btfd_allocations",
        "where_check": "snapshot_dt='2020-04-22'",
        "s3_write" : "s3://nike-dsm-planning-dev/IP_Test/upgrade_platform_test/results/",
        "email_id": "jerry.cheruvathoor@nike.com"
        }
    params = json.dumps(argv)
    
    try:
        params_dict = json.loads(params)
    except ValueError:
        raise Exception("Input parameter is NOT a valid JSON string: " + params)
    print(params_dict)
    main(params_dict)
