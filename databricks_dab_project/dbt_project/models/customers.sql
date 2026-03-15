
SELECT
id,
name,
surname,
CONCAT(name, ' ', surname) AS full_name,
address,
city,
state,
zipcode
FROM {{ source('default', 'customer') }}
WHERE id IS NOT NULL