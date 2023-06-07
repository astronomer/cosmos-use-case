select 
    "YEAR", "COUNTRY", "SOLAR_CAPACITY", "TOTAL_CAPACITY", "RENEWABLES_CAPACITY"
from SANDBOX.TAMARAFINGERLIN.energy
where "COUNTRY" = '{{ var("country_code") }}'