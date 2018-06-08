select
  max(case when name = 'name1' then id end) name1,
  max(case when name = 'name2' then id end) name2,
  max(case when name = 'name3' then id end) name3
from test;