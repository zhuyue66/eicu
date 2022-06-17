from postgresql import PostgreSQL
import psycopg2

# P/F <= 300 持续8小时
# PEEP >= 5
# ICD-9 respiratory failure
# ICD-9 congestive heart failure
# age > 18
# mechanical ventilation during ICU stay


connection = psycopg2.connect(database="eicu", user="postgres", password="123456", host="172.16.60.173", port="3307")
# 获得游标对象
cursor = connection.cursor()
sql = "set search_path to 'eicu_crd'"
cursor.execute(sql)
print('aaa')


