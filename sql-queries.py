from pyspark import SparkContext, SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

directory = '/some-directory'
data = sqlContext.read.parquet(directory, header=True)

data.createOrReplaceTempView("data")
sqlContext.table("data").cache

# Quantos filmes estão disponíveis na Amazon/Netflix?
query = '''
           SELECT Company, COUNT(DISTINCT(Title)) 
           FROM data 
           GROUP BY Company
        '''

sqlContext.sql(query).show() 

# Dos filmes disponíveis na Amazon, quantos % estão disponíveis na Netflix?¶

query = '''
        SELECT  (SELECT COUNT(DISTINCT Title)
                FROM data
                WHERE Company == 'Netflix' AND Title IN ( SELECT DISTINCT Title 
                                                          FROM data
                                                          WHERE Company == 'Amazon')) / COUNT(DISTINCT Title)
        FROM data
        WHERE Company == 'Amazon'
        '''
sqlContext.sql(query).show() 

# O quão perto a média das notas dos filmes disponíveis na Amazon está dos filmes disponíveis na Netflix?

query = '''
        SELECT Company, AVG(Rating)
        FROM data
        GROUP BY Company
        '''
sqlContext.sql(query).show() 

# Qual ano de lançamento possui mais filmes na Amazon/Netflix?

query = '''
SELECT Company, Year
FROM   

                   (SELECT Year, Company,
                   ROW_NUMBER() OVER (PARTITION BY Company ORDER BY Count DESC) rank
                    FROM 
                              (SELECT DISTINCT(Title), Year, Count(Year) Count, Company
                               FROM data
                               GROUP BY Company, Title, Year)
    
                    ) a
WHERE a.rank = 1
    

        
        '''
sqlContext.sql(query).show()

# Quais filmes que não estão disponíveis no catálogo da Netflix foram melhor avaliados (notas 4 e 5)?

query = '''
        SELECT DISTINCT(Title)
        FROM data
        WHERE Rating > 3 AND Company == 'Amazon' AND Title NOT IN ( SELECT DISTINCT(Title)
                                                                     FROM data
                                                                     WHERE Company == 'Netflix')
                            
        '''
sqlContext.sql(query).show() 

# Quais filmes que não estão disponíveis no catálogo da Amazon foram melhor avaliados (notas 4 e 5)?¶

query = '''
        SELECT DISTINCT(Title)
        FROM data
        WHERE Rating > 3 AND Company == 'Netflix' AND Title NOT IN ( SELECT DISTINCT(Title)
                                                                     FROM data
                                                                     WHERE Company == 'Amazon')
                            
        '''
sqlContext.sql(query).show() 