
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

select StoreID
    , StoreName
    , Location
    , CASE WHEN LEFT(StoreName,4)='ACME' THEN 'ACME Inc' ELSE 'John Simon Ltd' END AS Company
from {{ source('sales','stores') }}

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
