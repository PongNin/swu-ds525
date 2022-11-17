# Creating and Scheduling Data Pipelines

## Data model
![er](./Picture%20ref/Screenshot%202022-11-17%20230314.png)
<br>

## Project implementation instruction
<br>

### Get started by change directory
```sh
$ cd 05-creating-and-scheduling-data-pipelines
```

### Prepare environment workspace by Docker:

ถ้าใช้งานระบบที่เป็น Linux ให้เรารันคำสั่งด้านล่างนี้ก่อน

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

หลังจากนั้นให้รัน

```sh
docker-compose up
```

### Prepare data:

เสร็จแล้วให้คัดลอกโฟลเดอร์ `data` ที่เตรียมไว้ข้างนอกสุด เข้ามาใส่ในโฟลเดอร์ `dags` เพื่อที่ Airflow จะได้เห็นไฟล์ข้อมูลเหล่านี้ แล้วจึงค่อยทำโปรเจคต่อ

**หมายเหตุ:** จริง ๆ แล้วเราสามารถเอาโฟลเดอร์ `data` ไว้ที่ไหนก็ได้ที่ Airflow ที่เรารันเข้าถึงได้ แต่เพื่อความง่ายสำหรับโปรเจคนี้ เราจะนำเอาโฟลเดอร์ `data` ไว้ในโฟลเดอร์ `dags` เลย


### Access to airflow through web service:

เราจะสามารถเข้าไปที่หน้า Airflow UI ได้ที่ port 8080


### Access to SQLPad through web service:

จากที่ Postgres port มีปัญหาทำให้เราทำการเปลี่ยนโค้ดมาใช้ของทาง sqlpad แทน แต่อยู่ที่ port 3000


### Setup Postgres parameter for Airflow:

![er](./Picture%20ref/Screenshot%202022-11-17%20224932.png)
<br>
![er](./Picture%20ref/Screenshot%202022-11-17%20231927.png)
<br>

### Data validation:

ตรวจสอบการทำงานของ Airflow schedule ที่ตั้งค่าไว้
![er](./Picture%20ref/Screenshot%202022-11-17%20224814.png)
<br>

โดยมี graph flow ตามนี้
![er](./Picture%20ref/Screenshot%202022-11-17%20224839.png)
<br>

- ตรวจสอบข้อมูลที่มีการ load เข้าสู่ tables ตาม schedule ที่กำหนดไว้
![er](./Picture%20ref/Screenshot%202022-11-17%20225049.png)
<br>