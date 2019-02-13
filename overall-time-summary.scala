//Display the results by adding all the personal activity columns,
//adding all work activity columns
//adding all leisure activity columns and grouping by user id
display(grouped_df.select($"tucaseid",$"time_on_personal_activities", 
                                $"time_on_work_activities", 
                                $"time_on_leisure_activities")
                                .groupBy($"tucaseid").agg(sum($"time_on_personal_activities") as "personal_activities_time",
                                sum($"time_on_work_activities") as "work_time",
                                sum($"time_on_leisure_activities") as "leisure_time"))
                                
     
