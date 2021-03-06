create external table hanwen1_weather_delays_by_route (
  route string,
  clear_flights bigint, clear_delays bigint,
  fog_flights bigint, fog_delay bigint,
  rain_flights bigint, rain_delay bigint,
  snow_flights bigint, snow_delay bigint,
  hail_flights bigint, hail_delay bigint,
  thunder_flights bigint, thunder_delay bigint,
  tornado_flights bigint, tornado_delay bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,delay:clear_flights#b,delay:clear_delays#b,delay:fog_flights#b,delay:fog_delays#b,delay:rain_flights#b,delay:rain_delays#b,delay:snow_flights#b,delay:snow_delays#b,delay:hail_flights#b,delay:hail_delays#b,delay:thunder_flights#b,delay:thunder_delays#b,delay:tornado_flights#b,delay:tornado_delays#b')
TBLPROPERTIES ('hbase.table.name' = 'hanwen1_weather_delays_by_route');


insert overwrite table hanwen1_weather_delays_by_route
select concat(origin_name,dest_name),
  clear_flights, clear_delays,
  fog_flights, fog_delays,
  rain_flights, rain_delays,
  snow_flights, snow_delays,
  hail_flights, hail_delays,
  thunder_flights, thunder_delays,
  tornado_flights, tornado_delays from route_delays;
se