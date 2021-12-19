##############################################################################
# |ATIVIDADE 17 - UTLIZAÇÃO DO PYSPARK PARA ANALISE DE DADOS GOVERNAMENTAIS |#
#-+-------------------------------------------------------------------------+#
# |    AUTORES      |                                                        #
#-+-----------------+                                                        #
#-|    IVAN         |                                                        #
#-+-----------------+                                                        #
#-|    ROBSON       |                                                        #
#-+-----------------+                                                        #
#-|    PATRICCIA    |                                                        #
#-+-----------------+                                                        #
##############################################################################

###############################################################################################################################################################
# | CARACTERISTICAS DA ATIVIDADE |                                                                                                                            #
#  Utilize os dados do portal da transparência para:                                                                                                          #
#- correlacionar todos os dados disponíveis em relação a uma única pessoa, ex: forneça seu nome e o programa localize todas as referencias a este             #
#- correlacionar todos os dados disponíveis em relação a uma única instituição, ex: forneça um ministério e o programa localize todas as referencias a este   #
#                                                                                                                                                             #
#- liste os dados faltantes / não válidos                                                                                                                     #
#                                                                                                                                                             #
#- Desafio:                                                                                                                                                   #
#- utilize uma vpn para conectar todos os membros da equipe, gere um cluster e processe em cluster os dados                                                   #
###############################################################################################################################################################

###########################################
#            | FUNÇÕES |                  #
# 1 - puxar todas as informaçõe do robson #
# 2 - valor total pago pelo exercito      #
# 3 - media salarial                      #
# 4 - maior salario                       #
# 5 - menor salario                       #
#                                         #
###########################################


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as B
from pyspark.sql.functions import avg, max, min, col, count,regexp_replace, sum
from datetime import datetime
import math
import logging
import os

if  not os.path.isdir('logs'):
    os.mkdir('logs')

logging.basicConfig(filename=r"./logs/{}.log".format(datetime.now().strftime("%d-%m-%y_%H-%M")),
                    format='%(asctime)s %(filename)s %(levelname)s:%(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S',
                    encoding='utf-8',
                    level='DEBUG'                    
                    )

objeto_logger=logging.getLogger(__name__)

if __name__ == ('__main__'):
	spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
	objeto_logger.debug("Program initialized")

# LENDO CSV (JAN/2013) ------------------------------------------
	try:
		dados_jan = spark.read.format("csv")\
				.option("header","true")\
				.option("delimiter",";")\
				.option("inferSchema","true")\
				.load(r"C:\spark\201301_Remuneracao.csv")
		objeto_logger.info("File name format csv imported")
		
	except Exception as e:
		print('Erro ao concatenar --> ', e)	
	
	# LENDO CSV (FEV/2013) ------------------------------------------
	try: 
		dados_fev = spark.read.format("csv")\
				.option("header","true")\
				.option("delimiter",";")\
				.option("inferSchema","true")\
				.load(r"C:\spark\201302_Remuneracao.csv")
		objeto_logger.info("File name format csv imported")	
	except Exception as e:
		print('Erro ao concatenar --> ', e)	

	# LENDO CSV (MAR/2013) ------------------------------------------
	try: 	
		dados_mar = spark.read.format("csv")\
				.option("header","true")\
				.option("delimiter",";")\
				.option("inferSchema","true")\
				.load(r"C:\spark\201303_Remuneracao.csv")
		objeto_logger.info("File name format csv imported")
	except Exception as e:
		print('Erro ao concatenar --> ', e)		
	
	# LENDO CSV (ABR/2013) -------------------------------------------
 
	try: 	
		dados_abr = spark.read.format("csv")\
				.option("header","true")\
				.option("delimiter",";")\
				.option("inferSchema","true")\
				.load(r"C:\spark\201304_Remuneracao.csv")
		objeto_logger.info("File name formta csv imported")
	except Exception as e:
		print('Erro ao concatenar --> ', e) 
	
	# LENDO CSV (MAI/2013) -------------------------------------------
 	
	try:
		dados_mai = spark.read.format("csv")\
				.option("header","true")\
				.option("delimiter",";")\
				.option("inferSchema","true")\
				.load(r"C:\spark\201305_Remuneracao.csv")
		objeto_logger.info("File name format csv imported")
	except Exception as e:
		print('Erro ao concatenar --> ', e)			
	
	# LENDO CSV (JUN/2013) -------------------------------------------
 
	try:
		dados_jun = spark.read.format("csv")\
				.option("header","true")\
				.option("delimiter",";")\
				.option("inferSchema","true")\
				.load(r"C:\spark\201306_Remuneracao.csv")
		objeto_logger.info("File name format csv imported")
	except Exception as e:
		print('Erro ao concatenar --> ', e)	
	objeto_logger.info("File name format csv imported")
	
	# CONCATENANDO TODOS OS ARQUIVOS ---------------------------------
	
	try:
		dados_12     = dados_jan.union(dados_fev)
		dados_123    = dados_12.union(dados_mar)
		dados_1234   = dados_123.union(dados_abr)
		dados_12345  = dados_1234.union(dados_mai)
		dados_concat = dados_12345.union(dados_jun)
		objeto_logger.info("File concatenation")
  
	except Exception as e:
		print('Erro ao concatenar --> ', e)
  
	# TRATANDO AS COLUNAS ---------------------------------
 			
	try:	
		df_tratado = dados_concat.select('Id_SERVIDOR_PORTAL','NOME', 'REMUNERAÇÃO BÁSICA BRUTA (R$)', 'OUTRAS REMUNERAÇÕES EVENTUAIS (R$)', 'IRRF (R$)', 'PENSÃO MILITAR (R$)', 'REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)')
		df_tratado1 = df_tratado.select(col("Id_SERVIDOR_PORTAL").alias("ID") \
			,col("NOME").alias("NOME") \
			,col("REMUNERAÇÃO BÁSICA BRUTA (R$)").alias("REM_BRUTA") \
			,col("OUTRAS REMUNERAÇÕES EVENTUAIS (R$)").alias("OUTRAS_REM") \
			,col("IRRF (R$)").alias("IRRF")\
			,col("PENSÃO MILITAR (R$)").alias("PENSAO")\
			,col("REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)").alias('REM_LIQ')) 

		df_tratado1 = df_tratado1.withColumn("REM_BRUTA", regexp_replace("REM_BRUTA",  ","  ,"."))
		df_tratado1 = df_tratado1.withColumn("OUTRAS_REM", regexp_replace("OUTRAS_REM",  ","  ,"."))
		df_tratado1 = df_tratado1.withColumn("IRRF", regexp_replace("IRRF",  ","  ,"."))
		df_tratado1 = df_tratado1.withColumn("PENSAO", regexp_replace("PENSAO",  ","  ,"."))
		df_tratado1 = df_tratado1.withColumn("REM_LIQ", regexp_replace("REM_LIQ",  ","  ,"."))
		objeto_logger.info("File concatenation with error handling")
  
	except Exception as e:
		print('Erro no tratamento --> ', e)
    
	try:  
		df_tratado1.write.parquet(r"C:\spark\atividade_cluster")
		dados_parquet = spark.read.parquet(r"C:\spark\atividade_cluster")
		objeto_logger.info("Trsnaforming parquet") 
	except Exception as e:
		print(str(e))
	
	try:
     
		calculos = dados_parquet.groupBy('NOME')\
		.agg(max("REM_LIQ").alias('R_MAX')
		, min("REM_LIQ").alias('R_MIN')\
		, avg("REM_LIQ").alias('MEDIA')\
		, sum("REM_LIQ").alias('total_recebido')\
		,count("REM_LIQ").alias('QTD'))
		calculos.show(50, truncate = False) 
		objeto_logger.info("Calculation performed") 
      
		calculos1 = dados_parquet.groupBy('NOME')\
		.agg(max("PENSAO").alias('P_MAX')
		, min("PENSAO").alias('P_MIN')\
		, avg("PENSAO").alias('media')\
		, sum("PENSAO").alias('total_pago')\
		,count("PENSAO").alias('QTD'))
		calculos1.show(50, truncate = False) 
		objeto_logger.info("Calculation performed")
  
		calculos2 = dados_parquet.groupBy('NOME')\
		.agg(max("IRRF").alias('IR_MAX')
		, min("IRRF").alias('IR_MIN')\
		, avg("IRRF").alias('media')\
		, sum("IRRF").alias('total_pago')\
		,count("IRRF").alias('QTD'))
		calculos2.show(50, truncate = False) 
		objeto_logger.info("Calculation performed")
  
		calculos3 = dados_parquet.groupBy('NOME')\
		.agg(max("REM_BRUTA").alias('RB_MAX')
		, min("REM_BRUTA").alias('RB_MIN')\
		, avg("REM_BRUTA").alias('media')\
		, sum("REM_BRUTA").alias('total_recebido')\
		,count("REM_BRUTA").alias('QTD'))
		calculos3.show(50, truncate = False) 
		objeto_logger.info("Calculation performed")
  
	except Exception as e:
		print('Erro no tratamento --> ', e)
  
	
  
	try:
	
		print("-----------------------------------------------------")
		calculos.filter(calculos['NOME'] == "ROBSON JOSE MOTTA LOPES").show(truncate=False)
		calculos1.filter(calculos1['NOME'] == "ROBSON JOSE MOTTA LOPES").show(truncate=False)
		calculos2.filter(calculos2['NOME'] == "ROBSON JOSE MOTTA LOPES").show(truncate=False)
		calculos3.filter(calculos3['NOME'] == "ROBSON JOSE MOTTA LOPES").show(truncate=False)
		objeto_logger.info("serch name")
  
		print("-----------------------------------------------------")
		dados_parquet.agg({"REM_LIQ": "max"}).show(truncate=False)
		dados_parquet.agg({"PENSAO": "max"}).show(truncate=False)
		dados_parquet.agg({"REM_BRUTA": "max"}).show(truncate=False)
		objeto_logger.info("serch max")
  
		print("-----------------------------------------------------")
		dados_parquet.agg({"REM_LIQ": "min"}).show(truncate=False)
		dados_parquet.agg({"PENSAO": "min"}).show(truncate=False)
		dados_parquet.agg({"REM_BRUTA": "min"}).show(truncate=False)
		objeto_logger.info("serch min")
  
		print("-----------------------------------------------------")
		dados_parquet.agg({"REM_LIQ": "sum"}).show(truncate=False) 
		dados_parquet.agg({"PENSAO": "sum"}).show(truncate=False)
		dados_parquet.agg({"REM_BRUTA": "sum"}).show(truncate=False)
		objeto_logger.info("serch sum")
  
		print("-----------------------------------------------------")
		dados_parquet.agg({"REM_LIQ": "mean"}).show(truncate=False)
		dados_parquet.agg({"PENSAO": "mean"}).show(truncate=False)
		dados_parquet.agg({"REM_BRUTA": "mean"}).show(truncate=False)
		objeto_logger.info("serch mean")

	except Exception as e:
		print('Erro de calculos --> ', e)
  
    
 	






	# print(df_tratado1.show())