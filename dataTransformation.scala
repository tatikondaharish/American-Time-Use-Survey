import org.apache.spark.sql.functions._


// Read activity_summary_file
val activity_sum_file = spark.read.format("csv").
                                    option("header",true).
                                    option("inferSchema",true).
                                    load("dbfs:/mnt/America-time-use-survey/America-time-use-survey/atussum.csv")


//Grouping all the personal activity columns together
val personal_activities = List(col("t010101"),col("t110101"),col("t110199"),col("t119999"))

// COMMAND ----------

//Grouping all work activities together
val work_activities = List(col("t050101"),col("t050102"),col("t050103"),col("t050189"),col("t050201"),
                          col("t050202"),col("t050203"),col("t050204"),col("t050289"),col("t050301"),col("t050302"),
                          col("t050303"),col("t050304"),col("t050389"),col("t059999"))

// COMMAND ----------

//Group all leisure activities together
val leisure_activities = List(col("t120101"),col("t120199"),col("t120201"),col("t120202"),col("t120299"),col("t120301"),
                         col("t120302"),col("t120303"),col("t120304"),col("t120305"),col("t120306"),col("t120307"),col("t120308")
                        ,col("t120309"),col("t120310"),col("t120311"),col("t120312"),col("t120313"),col("t120399"),col("t120401"),
                         col("t120402"),col("t120403"),col("t120404"),col("t120405"), col("t120499"))

// COMMAND ----------

//Adding all personal activity columns, Adding all workactivity columns, adding all leisure activity columns
val grouped_df = activity_sum_file.select('*,personal_activities.reduce(_+_) as "time_on_personal_activities", 
                                work_activities.reduce(_+_) as "time_on_work_activities", 
                                leisure_activities.reduce(_+_) as "time_on_leisure_activities")
