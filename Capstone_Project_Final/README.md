# Final Capstone project

# Documentation: [link](https://github.com/pongthanin/swu-ds525/blob/main/Capstone_Project_Final/Document/Capstone%20project_%20Brazilian%20E-Commerce%20sales%20analysis%20-%20Google%20Docs.pdf)

## Dataset

### Brazilian E-Commerce Public Dataset by Olist : [link](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download)

## Data Schema

![er](./Picture%20ref/HRhd2Y0.png)
<br>

## Questions
Mainly foucsing on selling report which we would like to asking the following questions:
1. How is the total sales amount compare in term of product catagories and each state
2. Average sales per orders in each state 
3. Each payment type sales proportion 


## Data model

![er](./Picture%20ref/Screenshot%202022-12-17%20214306.png)
<br>


## Project implementation - Preparation

### 1. Get started
```sh
$ cd Capstone_Project_Final
```


### 2. Applying code for saving jupyter lab (Any update on coding)

```sh
sudo chmod 777 .
```


### 3. Prepare environment workspace by Docker (Airflow):

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```


### 4. create visual environment & install required libraries
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```


### 5. Start  environment

```sh
docker-compose up
```



## Starting AWS cloud service

### 1. Create AWS S3 bucket


### 2. Upload raw data into S3 bucket

![er](./Picture%20ref/Screenshot%202022-12-17%20130228.png)
<br>

Noted: From assumption, normally raw data (All tables) will be manually uploaded to S3 on monthly basis 
<br>

### 3. Get access key from AWS

Retrieve credential thru AWS terminal

```sh
cat ~/.aws/credentials
```
Then, copy these access key (Used to link with S3 and Redshift)
1) aws_access_key_id 
2) aws_secret_access_key
3) aws_session_token

![er](./Picture%20ref/Screenshot%202022-12-18%20201539.png)
<br>


## Project implementation - Transformation

By using pyspark

Target: To transform data from 8 tables into a final table that contains with all necessary features by following up data model
<br>

### 1. Access into working port 8888

Login with lab token

![er](./Picture%20ref/Screenshot%202022-10-05%20220731.png)
<br>

### 2. Excute notebook "etl.datalake_S3.ipynb" Step by step

![er](./Picture%20ref/Screenshot%202022-12-17%20222417.png)
<br>

*Transform data into "final_table" before uploading result back to S3 by partition it with "year"

![er](./Picture%20ref/Screenshot%202022-12-17%20130251.png)
<br>

Code: [python_code_for_create_final_table](https://github.com/pongthanin/swu-ds525/blob/main/Capstone_Project_Final/etl_datalake_S3.ipynb)
<br>

### Result after merging table
![er](./Picture%20ref/Screenshot%202022-12-17%20223352.png)
<br>


## Project implementation - data warehouse

We use airflow to implement in this step for copy table into Redshift

### 1.Create Cluster Redshift

![er](./Picture%20ref/Screenshot%202022-12-17%20130045.png)
<br>

### 2. Execute the "Data warehouse" process thru Airflow:

1) Copy "Endpoint" and Cluster information to update Redshift credential
![er](./Picture%20ref/Screenshot%202022-12-18%20135245.png)
<br>

2) Access Airflow UI by port 8080 (localhost:8080) with below credential
> - Username: "airflow"<br>
> - Password: "xxxxxxx"<br>

3) The Datawarehouse script will be run follow this flow and the schedule configuration

![er](./Picture%20ref/Screenshot%202022-12-17%20130540.png)
<br>

> - Schedule: "Monthly" (1st of each month)<br>
> - Start date: "1st February 2017"

![er](./Picture%20ref/Screenshot%202022-12-17%20130528.png)
<br>

4) The data will be loaded into Redshift (check by Query editor)
```sh
select * from final_table;
```
![er](./Picture%20ref/Screenshot%202022-12-17%20125758.png)
<br>
![er](./Picture%20ref/Screenshot%202022-12-17%20125839.png)
<br>

## Project implementation - Tableau Dashboard

1) Connect Tableau desktop with AWS Redshift by using this credential
![er](./Picture%20ref/Screenshot%202022-12-18%20140105.png)
<br>

2) Load the data from Redshift to Tableau
![er](./Picture%20ref/Screenshot%202022-12-18%20140211.png)
<br>

### Final dashboard after implement thru all process
- Dashboard: [Dashboard_final_capstone](https://public.tableau.com/app/profile/pongthanin.wangkiat/viz/Dashboard_final_capstone/Dashboard1)

![er](./Picture%20ref/Screenshot%202022-12-18%20191543.png)
<br>

# Project presentation slide

- Presentation: [slide](https://www.canva.com/design/DAFVGbsjpfY/5PK9Jd5Jx7bR1qdtmyfY-Q/view?utm_content=DAFVGbsjpfY&utm_campaign=designshare&utm_medium=link&utm_source=publishsharelink)
<br>

__________
<br>

## Shutdown steps
##### 1. Stop services by shutdown Docker:
```sh
$ docker-compose down
```

##### 2. Deactivate the virtual environment:
```sh
$ deactivate
```