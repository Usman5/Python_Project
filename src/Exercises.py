from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, desc, col

# Initialize SparkSession
spark = SparkSession.builder.appName("CSV_Shape").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

csv_file_path = "hdfs:////tmp/US_UK_05052025/usman_younas/employees.csv"

# Read CSV into Spark DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df.show()

row_count = df.count()
col_count = len(df.columns)
print(row_count,col_count)

mean_value = df.select(mean("Salary")).collect()[0][0]
print(mean_value)

stddev_value = df.select(stddev("Salary")).collect()[0][0]
print(stddev_value)

median = df.approxQuantile("Salary", [0.5], 0.01)[0]
print(median)

mode_row = df.groupBy("Salary").count().orderBy(desc("count")).first()

if mode_row:
    mode_value = mode_row[0]
    print("Mode of Salary:", mode_value)
else:
    print("No mode found")

print(df.types)

df = df.withColumnRenamed('EmployeeID', 'Emp_ID')

df.show()

filtered_df = df.filter(df.Salary > 50000)
filtered_df.show()

selected_df = df.select('Name', 'Salary')
selected_df.show()

# df = df.drop('Department')
# df.show()

df = df.withColumn('Salary', col('Salary') + 500)
df.show()

# df = df.withColumn('Updated Salary', col('Salary') * 1.2)
# df.show()

sorted_df = df.orderBy('Salary')
sorted_df.show()

# Stop Spark session
spark.stop()
