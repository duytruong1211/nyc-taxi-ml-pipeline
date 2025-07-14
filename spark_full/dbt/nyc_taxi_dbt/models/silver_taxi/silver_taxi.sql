select
    t.*,
    pu_zone.Zone AS PU_Zone,
    pu_zone.Borough AS PU_Borough,
    pu_zone.service_zone AS PU_ServiceZone,
    do_zone.Zone AS DO_Zone,
    do_zone.Borough AS DO_Borough,
    do_zone.service_zone AS DO_ServiceZone,

    UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime) AS trip_duration_seconds,
    case when trip_distance/trip_duration_seconds >= 0.02 then 1 else 0 end as is_speed_suspicious
FROM 
    {{ ref('bronze_raw') }} t
    LEFT JOIN {{ ref('taxi_zone_lookup') }} pu_zone ON t.PULocationID = pu_zone.LocationID
    LEFT JOIN {{ ref('taxi_zone_lookup') }} do_zone ON t.DOLocationID = do_zone.LocationID


where
    --- remove corrupted data
    (RatecodeID is not null or passenger_count is not null)
    --- taxi ride in NYC go up to 6 passengers
    and passenger_count < 7
    --- trip distance is non 0  and less than 100 miles,  quantile 999 at 30 miles (corrupted data)
    and trip_distance >0 
    and trip_distance <100
    --- only within acceptable range
    and RatecodeID BETWEEN 1 AND 6
    --- excluded negative/zero fares (refunds, disputes), and extreme outlier >99%
    and fare_amount >0 and fare_amount <200
    --- excluded error payment types
    and payment_type BETWEEN 1 AND 4
    --- only include make sense-ish tips amount
    and tip_amount >= 0 AND tip_amount < 500
    --- hard cap to be >0
    AND extra >= 0
    --- only true values for mta tax are 0 and 0.5
    AND mta_tax in (0,0.5)
    --- extreme p99 outliers go up to 6.94
    AND tolls_amount >= 0 and tolls_amount < 50
    -- same case, only 0 and 1
    AND improvement_surcharge in (0,1)
    --- cap at extreme value, high end p999 is at 163
    AND total_amount BETWEEN 0 AND 200

    AND congestion_surcharge >= 0
    AND airport_fee >= 0

    AND tpep_pickup_datetime >= '2023-01-01'
    and tpep_dropoff_datetime > tpep_pickup_datetime
    and tpep_pickup_datetime < '2025-01-01'


    and UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime) BETWEEN 60 AND 3600