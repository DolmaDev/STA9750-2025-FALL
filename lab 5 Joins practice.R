
flights |>
  inner_join(airlines, join_by(carrier==carrier)) |>
  inner_join(airports,
             join_by(origin==faa),
             suffix = c("_airline", "_origin_airport"))
west_coast_airports <- airports |>
  filter(tzone == "America/Los_Angeles")
  inner_join(flights, west_coast_airports, join_by(dest == faa))

inner_join(flights, west_coast_airports, join_by(dest == faa)) |>
  summarize(mean(arr_delay, na.rm=TRUE))

inner_join(flights, airports, join_by(dest == faa)) |>
  filter(tzone == "America/Los_Angeles") |>
  drop_na() |>
  summarize(mean(arr_delay, na.rm=TRUE))

inner_join(flights, 
           airports |> filter(tzone == "America/Los_Angeles"), 
           join_by(dest == faa)) |>
  drop_na() |>
  summarize(mean(arr_delay, na.rm=TRUE))

#Q: What is the name of the airline with the longest average departure delay 
#on a acloudy day?


flights|>
  filter(!is.na(dep_delay), 
         dep_delay>0) |>
  group_by(carrier) |>
  summarize(avg_dep_delay = mean(dep_delay))|>
  slice_max(avg_dep_delay)|>
  inner_join(airlines, join_by(carrier==carrier))|> 
  pull(name)


#Q: What is the name of the airline with the longest average departure delay?
