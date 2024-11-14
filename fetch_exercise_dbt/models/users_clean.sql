select
    state,
    role,
    active::boolean,
    _id::json ->> '$oid' as user_id,
    to_timestamp(
        ("createdDate"::json ->> '$date')::double precision / 1000
    ) as created_date,
    to_timestamp(
        ("lastLogin"::json ->> '$date')::double precision / 1000
    ) as last_login
from {{ source('raw_data', 'users') }}
