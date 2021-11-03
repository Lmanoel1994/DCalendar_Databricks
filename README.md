# Dimensão time usando apache spark no databricks  <br />

Campos da dimensão <br />
SK_DATETIME: Chave do dataframe (20210101) <br />
YEAR: Ano da data (2021)<br />
MONTH: Mes da data (1)<br />
DAY: Dia da data (1)<br />
USA_DATE: Data completa no formato americano ('2021-01-01)<br />
USA_DATE_NAME: Data americana em string ('January 01, 2021')<br />
USA_DAY_OF_WEEK_NAME: Nome do dia da semana em ingles ('Friday)<br />
USA_MONTH_NAME: Nome do mes em ingles ('January')<br />
USA_HOLYDAY: Campo onde informa se a data caiu em um feriado americano (1)<br />
BR_DATE: Data completa no formato brasileiro ('01-01-2021')<br />
BR_DATE_NAME: Data brasileira em string ('01 Janeiro de 2021')<br />
BR_DAY_OF_WEEK_NAME: Nome do dia da semana em portugues ('Sexta-feira')<br />
BR_MONTH_NAME: Nome do mes em ingles ('Janeiro')<br />
BR_HOLYDAY: Campo onde informa se a data caiu em um feriado no brasileiro (1)<br />

SK_DATETIME|YEAR|MONTH|DAY|USA_DATE  |USA_DATE_NAME     |USA_DAY_OF_WEEK_NAME|USA_MONTH_NAME|USA_HOLYDAY|BR_DATE   |BR_DATE_NAME        |BR_DAY_OF_WEEK_NAME|BR_MONTH_NAME|BR_HOLYDAY|
---------  |----|-----|---|----------|------------------|--------------------|--------------|-----------|----------|--------------------|-------------------|-------------|----------|
20210101   |2021|1    |1  |2021-01-01|January 01, 2021  |Friday              |January       |1          |01-01-2021|01 Janeiro de 2021  |Sexta-feira        |Janeiro      |1         |
20210102   |2021|1    |2  |2021-01-02|January 02, 2021  |Saturday            |January       |0          |02-01-2021|02 Janeiro de 2021  |Sabado             |Janeiro      |0         |
20210103   |2021|1    |3  |2021-01-03|January 03, 2021  |Sunday              |January       |0          |03-01-2021|03 Janeiro de 2021  |Segunda-feira      |Janeiro      |0         |
20210104   |2021|1    |4  |2021-01-04|January 04, 2021  |Monday              |January       |0          |04-01-2021|04 Janeiro de 2021  |Domingo            |Janeiro      |0         |
20210105   |2021|1    |5  |2021-01-05|January 05, 2021  |Tuesday             |January       |0          |05-01-2021|05 Janeiro de 2021  |Terca-Feira        |Janeiro      |0         |
