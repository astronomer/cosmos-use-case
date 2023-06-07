select 
    "YEAR", "COUNTRY", "SOLAR_CAPACITY", "TOTAL_CAPACITY", "RENEWABLES_CAPACITY"
from postgres.postgres.energy
where "COUNTRY" = '{{ var("country_code") }}'