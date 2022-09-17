# Data Modeling III Building a Data Warehouse

## Data model
![er](./Picture%20ref/Screenshot%202022-09-17%20201259.png)
<br>

## Project implementation instruction

## Get started
```sh
$ cd 03-building-a-data-warehouse
```

## Install & activate environment

```sh
python -m venv ENV
source ENV/bin/activate
```

## install required libraries from config file (only 1st time): 
```sh
pip install -r requirements.txt
```

## Connect with AWS redshift
```sh
**Step 1:**
Crate a redshift claster
```
![Redshift](Picture%20ref/Screenshot%202022-09-17%20192425.png)
<br>

```sh
**Step 2:**
Upload file into S3
```
![S3](Picture%20ref/Screenshot%202022-09-17%20192213.png)
<br>

```sh
**Step 3:**
Open public access
```

## Config 'etl.py' to link with AWS Redshift
&nbsp;&nbsp;&nbsp;a. Host : copy from AWS Redshift endpoint <br>
&nbsp;&nbsp;&nbsp;b. Port : 5439 <br>
&nbsp;&nbsp;&nbsp;c. Dbname : dev <br>
&nbsp;&nbsp;&nbsp;d. User/Password : as define when create the cluster 
![Config](Picture%20ref/Screenshot%202022-09-17%20215825.png)
<br>

### Config ‘etl.py’ to copy the data from AWS S3 to AWS Redshift:
&nbsp;&nbsp;&nbsp;a. From : the URI to data file <br>
&nbsp;&nbsp;&nbsp;b. Credentials : the ARN of LabRole <br>
&nbsp;&nbsp;&nbsp;c. Json : the URI to manifest file <br>
![Copy](Picture%20ref/Screenshot%202022-09-17%20215423.png)
<br>

### Query data thru python script, named ‘etl.py’:
```sh
$ python etl.py
```

### Result

#### Events table
![Events](Picture%20ref/Screenshot%202022-09-17%20200021.png)
<br>

#### Actors table
![Actors](Picture%20ref/Screenshot%202022-09-17%20200054.png)
<br>

#### Orgs table
![Orgs](Picture%20ref/Screenshot%202022-09-17%20200155.png)
<br>

#### Repo table
![Repo](Picture%20ref/Screenshot%202022-09-17%20200123.png)
<br>