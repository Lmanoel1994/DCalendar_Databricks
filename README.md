# Dimensão Calendario usando apache spark (databricks)  <br />

##Data de exemplo: 2021-01-01
1. **SK_DATETIME**
   - Chave do dataframe 
        - Exemplo: 20210101 
       <br>
2. YEAR 
   - Ano da data 
        - Exemplo: 2021
        <br>
3. MONTH 
   - Mes da data 
        - Exemplo: 1 
        <br>
4. DAY
    -  Dia da data 
        - Exemplo: 1
        <br>
5. WEEK_NUMBER
    -  Numero da Semana que é referente a data
        - Exemplo: 53
        <br>
        
6. DATE
    - Data completa no formato padrão 
        - Exemplo: '2021-01-01
        <br>
        
7. USA_DAY_OF_WEEK_NAME
    - Nome da semana em ingles
        - Exemplo:  'Friday'
        <br>
        
        
8. USA_MONTH_NAME
    - Nome do mes em ingles 
        - Exemplo:  'January'
        <br>
        
9. USA_HOLIDAY
    - Campo onde informa se a data caiu em um feriado americano 
        - Exemplo: 1
        <br>
        
10. BR_DATE
    - Data completa no formato brasileiro 
        - Exemplo: '01-01-2021'
        <br>
        
11. BR_DAY_OF_WEEK_NAME
    - Nome da semana em português
      - Exemplo: 'Sexta feira'
      <br>

13. BR_MONTH_NAME
    - Nome do mes em portugues 
        - Exemplo: 'Janeiro'
       <br>
       
14. BR_HOLIDAY
    - Campo onde informa se a data caiu em um feriado no brasileiro 
        - Exemplo: 1


```python
#Instalando Biblioteca holidays
!pip install holidays
```
```python
#Import python
import holidays
import datetime as dt
```
```python
#Import pyspark
from pyspark.sql.functions import when, col, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
```
```python
#Data de inicio e fim
start = dt.datetime.strptime("2021-01-01", "%Y-%m-%d")
end = dt.datetime.strptime("2100-01-01", "%Y-%m-%d")

#Gerando uma lista com um range de data do inicio até o fim
date_generated = [start + dt.timedelta(days=x) for x in range(0, (end-start).days)]
```
```python
# Meses em português
month_br = {1:'Janeiro',2:'Fevereiro',3:'Marco',4:'Abril',5:'Maio',6:'Junho',
            7:'Julho',8:'Agosto',9:'Setembro',10:'Outubro',11:'Novembro',12:'Dezembro'}
```

```python
# Dia da semana em português
week_br = {'Monday':'Segunda-feira','Tuesday':'Terça-feira','Wednesday':'Quarta-feira',
'Thursday':'Quinta-feira','Friday':'Sexta-feira','Saturday':'Sabado','Sunday':'Domingo'}
```



```python
#Estruturando os dados do calendario
list_dates = []
for data in date_generated:
    list_dates.append((data.strftime("%Y%m%d"),
                  data.date(),
                  data.strftime("%d-%m-%Y"),
                  data.year,
                  data.month,
                  data.strftime("%d"),
                  data.strftime("%A"),
                  data.strftime("%B"),
                  data.date().isocalendar()[1],
                  int(data.date() in holidays.Brazil()),
                  int(data.date() in holidays.US())))
    
```
```python
#Traduzindo os meses de ingles para português
dates = [day + (month_br[day[4]], week_br[day[6]]) for day in list_dates]
```

```python
#Criando o schema do dataframe
dcalendar_schema = StructType([
  StructField('SK_DATETIME', StringType(), True),
  StructField('DATE', DateType(), True),
  StructField('BR_DATE', StringType(), True),
  StructField('YEAR', IntegerType(), True),
  StructField('MONTH', IntegerType(), True),
  StructField('DAY', StringType(), True),
  StructField('USA_DATE_NAME', StringType(), True),
  StructField('USA_DAY_OF_WEEK_NAME', StringType(), True),
  StructField('USA_MONTH_NAME', StringType(), True),
  StructField('WEEK_NUMBER', IntegerType(), True),
  StructField('BR_HOLIDAY', IntegerType(), True),
  StructField('USA_HOLIDAY', IntegerType(), True)])
```

```python
#Criando o dataframe
dcalendar_df = spark.createDataFrame(data = list_dates, schema = dcalendar_schema)
```

```python
#Ordenando as colunas 
dcalendar_df = dcalendar_df.select(
    col('SK_DATETIME'),
    col('YEAR'),
    col('MONTH'),
    col('DAY').cast(IntegerType()),
    col('WEEK_NUMBER'),
    col('DATE'),
    col('USA_DATE_NAME'),
    col('USA_DAY_OF_WEEK_NAME'),
    col('USA_MONTH_NAME'),col('USA_HOLIDAY'),
    col('BR_DATE'),col('BR_DATE_NAME'),
    col('BR_DAY_OF_WEEK_NAME'),
    col('BR_MONTH_NAME'),col('BR_HOLIDAY')
)
```
```python
#Dataframe finalizado
display(dcalendar_df.limit(10))
```
SK_DATETIME|YEAR|MONTH|DAY| WEEK_NUMBER |USA_DATE  |USA_DAY_OF_WEEK_NAME|USA_MONTH_NAME|USA_HOLIDAY|BR_DATE   |BR_DAY_OF_WEEK_NAME|BR_MONTH_NAME|BR_HOLIDAY|
---------  |----|-----|---| ------------ |----------|--------------------|--------------|-----------|----------|-------------------|-------------|----------|
20210101   |2021|1    |1  |      53      |2021-01-01|Friday              |January       |1          |01-01-2021|Sexta-feira        |Janeiro      |1         |
20210102   |2021|1    |2  |      53      |2021-01-02|Saturday            |January       |0          |02-01-2021|Sabado             |Janeiro      |0         |
20210103   |2021|1    |3  |      53      |2021-01-03|Sunday              |January       |0          |03-01-2021|Segunda-feira      |Janeiro      |0         |
20210104   |2021|1    |4  |       1      |2021-01-04|Monday              |January       |0          |04-01-2021|Domingo            |Janeiro      |0         |
20210105   |2021|1    |5  |       1      |2021-01-05|Tuesday             |January       |0          |05-01-2021|Terca-Feira        |Janeiro      |0         |

