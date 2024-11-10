select _id::json->>'$oid' as _id,
state,
to_timestamp(cast("createdDate"::json ->>'$date' as double precision)/1000) as created_date,
to_timestamp(cast("lastLogin"::json ->>'$date' as double precision)/1000) as last_login,
role,
active
from {{ source('raw_data', 'users') }}
