<h1> coffee_sales_pipeline </h1> 
<p align="center">
  <img src="./pics/coffee_sales_dag.png" width="1000"> <br>
  Sales Data Pipeline DAG
</p>

<h3> Create Cloud Composer (Airflow) and then set up the environment </h3>
<h3> Create dataset and table in BigQuery </h3>
<h3> Adding the Storage Object Admin role to the Cloud SQL service account </h3> 
<p>
  <img src="./pics/grant_access_cloudsql.png" width="400"> <br>
</p>

<h3> Upload the Airflow DAG file to the DAGS folder </h3> 
<p>
  <pre><code> gsutil cp coffee_sales_dag.py [GCS_BUCKET]/dags </code></pre>
</p>


<h2> Result </h2> 
<p>
  <img src="./pics/data_in_bq.png" width="1000"> <br>
  data has been successfully loaded into BigQuery(Data warehouse) <br><br>
  <img src="./pics/querydata.png" width="800"> <br>
  query data
</p>
