# Final Capstone project

## Dataset

### Brazilian E-Commerce Public Dataset by Olist : [link](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download)

## Data Schema

![er](./Picture%20ref/HRhd2Y0.png)
<br>

## Problem & Target
Mainly foucsing on selling report which we would like to asking the following questions:
1. How is the total sales amount compare in term of product catagories and each state
2. Average sales per orders in each state 
3. Each payment type sales proportion 

## Data model





## Project implementation instruction

## Get started
```sh
$ cd Capstone_Project_Final
```

## 1.Applying code for saving jupyter lab (Any update on coding)

```sh
sudo chmod 777 .
```

### 2.Prepare environment workspace by Docker (Airflow):
<br>

ถ้าใช้งานระบบที่เป็น Linux ให้เรารันคำสั่งด้านล่างนี้ก่อน

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## create visual environment & install required libraries
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## Start python environment

```sh
docker-compose up
```

## Get access key from AWS
```sh
cat ~/.aws/credentials
```