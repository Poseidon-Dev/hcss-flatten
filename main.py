import pandas as pd
from src.ecmsconn import JobQuery
from src.hcss import MergeHeavy, HCSSExport, HourCalculations

df = HourCalculations()
# data = HCSSExport('documentation\HcssAcctHDS.xlsx').process()

# print(df)


# df = MergeHeavy().save()
data = df.calc_non_ca_hours()
# df.save()
data = data[data['EMPLOYEENO'] == 10533]

print(data)
# print(data.dtypes)


