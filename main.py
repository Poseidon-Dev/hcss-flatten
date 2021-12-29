import pandas as pd
from src.ecmsconn import JobQuery
from src.hcss import MergeHeavy, HCSSExport, HourCalculations


## Singular Test
# df = HCSSExport('documentation\HcssAcctbhc.xlsx')
# merge = df.process()
# print(merge)


## Test All
df = HourCalculations('dumps\export.xlsx')
data = df.calc_ca_hours()


# data = data[data['DAILYFLAG'] == 1]

print(data)


