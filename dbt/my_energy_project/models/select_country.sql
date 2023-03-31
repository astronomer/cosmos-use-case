select 
    "YEAR", "COUNTRY", "SOLAR_CAPACITY", "TOTAL_CAPACITY", "RENEWABLES_CAPACITY"
from energy_db.energy_schema.energy
where "COUNTRY" = '{{ var("country_code") }}'