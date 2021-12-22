import pandas as pd
from src.ecmsconn import JobQuery
from src.hcss import MergeHeavy, HCSSExport

df = HCSSExport('documentation\HcssAcctCCT.xlsx')


df = df.export()
print(df)

# jq = JobQuery().to_df()

# joined = pd.merge(df, jq, how='left', on=['JOB', 'SUB'])
# joined.to_excel('dumps\export.xlsx', index=False, header=True)

