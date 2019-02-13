//Display the results based on grouping by sex and aggreating the perosnal activity column, 
//aggregating work_time, aggregating leisure time
display(grouped_df.withColumn("sex",when(col("tesex")equalTo("1"),"Male").otherwise("Female"))
            .select($"sex",$"tucaseid",$"time_on_personal_activities", $"time_on_work_activities",
                    $"time_on_leisure_activities").groupBy($"sex").agg(sum($"time_on_personal_activities") 
                    as "personal_activities_time",
                    sum($"time_on_work_activities") as "work_time",
                    sum($"time_on_leisure_activities") as "leisure_time",
                    sum($"time_on_personal_activities"+$"time_on_work_activities"+$"time_on_leisure_activities") as "total_time"))
