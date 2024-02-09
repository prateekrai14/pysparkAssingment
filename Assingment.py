from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import avg
from pyspark.sql.functions import col, sqrt, pow
import folium

# Create a Spark session
spark = SparkSession.builder.appName("Assingment").getOrCreate()

# Load the dataset 
df = spark.read.csv("/content/database.csv", header=True)

# 2.Convert the Date and Time columns into a timestamp column named Timestamp.
df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")),"MM/dd/yyyy HH:mm:ss"))


def categorize_magnitude(magnitude):
    magnitude = float(magnitude)
    if magnitude < 4.0:
        return "Low"
    elif magnitude < 7.0:
        return "Moderate"
    else:
        return "High"



# Register the UDF
categorize_magnitude_udf = udf(categorize_magnitude, StringType())

# 5.Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.
df=df.withColumn("earthquake_level", categorize_magnitude_udf(df["magnitude"]))

#4.Calculate the average depth and magnitude of earthquakes for each earthquake type.
avg_depth_magnitude=df.groupBy("Type").agg(avg("Depth").alias("average_depth"),avg("Magnitude").alias("average_magnitude"))

#6.Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).
df = df.withColumn("distance_from_ref",sqrt(pow(col("latitude") - 0, 2) + pow(col("longitude") - 0, 2)))

m = folium.Map(location=[0, 0], zoom_start=2)

for row in df.collect():
    folium.Marker([float(row['Latitude']), float(row['Longitude'])],
                  popup=f"Magnitude: {row['Magnitude']}, Depth: {row['Depth']}").add_to(m)

# 7.Visualize the geographical distribution of earthquakes on a world map using appropriate libraries  Folium).
m.save('earthquake_map.html')
