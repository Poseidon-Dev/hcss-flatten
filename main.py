import pandas as pd
from src.ecmsconn import JobQuery
from src.hcss import MergeHeavy, HCSSExport, HourCalculations

df = HourCalculations()
# data = HCSSExport('documentation\HcssAcctHDS.xlsx').process()

# print(df)


# df = MergeHeavy().save()
data = df.all_employees
# df.save()
data = data[data['EMPLOYEENO'] == 12799]

print(data)
# print(data.dtypes)


