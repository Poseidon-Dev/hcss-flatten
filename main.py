import pandas as pd
from src.ecmsconn import JobQuery
from src.hcss import MergeHeavy, HCSSExport


## Singular Test
# df = HCSSExport('documentation\HcssAcctbhc.xlsx')
# merge = df.process()
# print(merge)


## Test All
df = MergeHeavy()
df.save()
