# Analytics Engineering

## 1.Start python environment

```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## 2.Create a dbt project

```sh
dbt init
```

<br>
Enter a name for your project: jaffle
<br>

## 3.Edit the dbt profiles

```sh
code ~/.dbt/profiles.yml
```

```sh
jaffle:
  outputs:

    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: public

    prod:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: prod

  target: dev
```

### Test dbt connection

```sh
cd jaffle
dbt debug
```

You should see "All checks passed!".

## 4.To create models

```sh
dbt run
```

## 5.To test models

```sh
dbt test
```

## 6.To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve
```

## Final Lineage Graph

![er](./Picture%20ref/Screenshot%202022-11-20%20180213.png)
<br>