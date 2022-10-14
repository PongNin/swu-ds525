# Building a Data Lake

## Data model
![er](./Picture%20ref/Screenshot%202022-09-17%20201259.png)
<br>

## Project implementation instruction

## Get started by change directory
```sh
$ cd 04-building-a-data-lake
```

## Applying code for saving jupyter lab (Any update on coding)

```sh
sudo chmod 777 .
```
## Running jupyter 

```sh
docker-compose up
```
## Access into working port 8888
![er](./Picture%20ref/Screenshot%202022-10-14%20233152.png)
<br>

## Excute notebook "etl.local.ipynb" Step by step
![er](./Picture%20ref/Screenshot%202022-10-14%20233502.png)
<br>

## Chcek the cleaned output data in folders

### actors : [actors](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/actors)

### repos : [repos](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/repos)

### orgs : [orgs](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/orgs)

### events : [events](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/events)
<br>


## To shutdown

To shutdown, press Ctrl+C and run:

```sh
docker-compose down
```