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
        
6. USA_DATE
    - Data completa no formato americano 
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
        
11. BBR_DAY_OF_WEEK_NAME
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
import datetime as
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
week_br = {'Monday':'Segunda-feira','Tuesday':'Terça-feira','Wednesday':'Quarta-feira','Thursday':'Quinta-feira','Friday':'Sexta-feira','Saturday':'Sabado','Sunday':'Domingo'}
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
#Criando o schema do dataframe
dcalendar_schema = StructType([
  StructField('SK_DATETIME', StringType(), True),
  StructField('USA_DATE', DateType(), True),
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
#Gerando um nova coluna chamada 'BR_MONTH_NAME'que traduz os nomes dos meses para o portugues do Brasil
dcalendar_df = dcalendar_df.withColumn(
    'BR_MONTH_NAME', 
    when(col('MONTH')=='01','Janeiro').
    when(col('MONTH')=='02','Fevereiro').
    when(col('MONTH')=='03','Marco').
    when(col('MONTH')=='04','Abril').
    when(col('MONTH')=='05','Maio').
    when(col('MONTH')=='06','Junho').
    when(col('MONTH')=='07','Julho').
    when(col('MONTH')=='08','Agosto').
    when(col('MONTH')=='09','Setembro').
    when(col('MONTH')=='10','Outubro').
    when(col('MONTH')=='11','Novembro').
    when(col('MONTH')=='12','Dezembro').otherwise(col('MONTH')))
```
```python
#Gerando um nova coluna chamada 'BR_DAY_OF_WEEK_NAME' que traduz os nomes dos dias para o portugues do Brasil
dcalendar_df = dcalendar_df.withColumn(
    'BR_DAY_OF_WEEK_NAME', 
    when(col('USA_DAY_OF_WEEK_NAME')=='Sunday','Segunda-feira').
    when(col('USA_DAY_OF_WEEK_NAME')=='Tuesday','Terca-Feira').
    when(col('USA_DAY_OF_WEEK_NAME')=='Wednesday','Quarta-feira').
    when(col('USA_DAY_OF_WEEK_NAME')=='Thursday','Quinta-feira').
    when(col('USA_DAY_OF_WEEK_NAME')=='Friday','Sexta-feira').
    when(col('USA_DAY_OF_WEEK_NAME')=='Saturday','Sabado').
    when(col('USA_DAY_OF_WEEK_NAME')=='Monday','Domingo').otherwise(col('USA_DAY_OF_WEEK_NAME')))
```
```python
#Gerando um nova coluna chamada 'BR_DATE_NAME' que traduz os nomes das datas para o portugues do Brasil
dcalendar_df = dcalendar_df.withColumn('BR_DATE_NAME',
    concat(col('DAY'), lit(' '),col('BR_MONTH_NAME'),lit(' de '),col('YEAR')))
```
```python
#Ordenando as colunas 
dcalendar_df = dcalendar_df.select(
    col('SK_DATETIME'),
    col('YEAR'),
    col('MONTH'),
    col('DAY').cast(IntegerType()),
    col('WEEK_NUMBER'),
    col('USA_DATE'),
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

