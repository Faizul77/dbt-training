select
--from raw_orders
{{ dbt_utils.generate_surrogate_key(['o.orderid', 'c.customerid', 'p.productid']) }}
orderid,
orderdate,
shipdate,
shipmode,
o.customerid,
o.productid,
ordersellingprice2,
ordercostprice,
--from raw_customer
customername,
segment,
country,
--from raw_product
category,
productname,
subcategory,
ordersellingprice2 - ordercostprice as orderprofit 
{{markup('ordersellingprice2','ordercostprice')}} as markup
from {{ ref('raw_orders') }} as o
left join {{ ref('raw_customer') }} as c
on o.customerid = c.customerid
left join {{ ref('raw_product') }} as p
on o.productid = p.productid