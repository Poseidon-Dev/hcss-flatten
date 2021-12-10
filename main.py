import pandas as pd
import os


class HCSSExport:

    def __init__(self, file_path):
        self.file_path = file_path
        self.cols = [
            'Employee Number',
            'Week Number',
            'Day of Week',
            'Project/Job Number',
            'Sub Project / Job Number',
            'Job Cost Distribution',
            'Regular Hours',
            'Overtime Hours',
            'Other Hours',
            'Department Number',
            'Week Ending Date',
            ]
        self.grouping = [
            'Employee Number',
            'Week Number',
            'Day of Week',
            'Project/Job Number',
            'Sub Project / Job Number',
            'Job Cost Distribution',
            'Department Number',
            'Week Ending Date',
            ]


    def data(self):
        return pd.read_excel(self.file_path)

    
    def subset(self, subset=[]):
        if not subset:
            subset = self.cols
        return self.data()[subset]


    def detail(self):
        detail = self.data()[self.cols].groupby(self.grouping, group_keys=True).agg(
            Regular=pd.NamedAgg(column='Regular Hours', aggfunc='sum'),
            Overtime=pd.NamedAgg(column='Overtime Hours', aggfunc='sum'),
            Other=pd.NamedAgg(column='Other Hours', aggfunc='sum'),
        ).reset_index()
        detail['Total'] = detail['Regular'] + detail['Overtime'] + detail['Other']
        detail['Regular'] = detail['Regular'].astype(float)
        detail['Overtime'] = detail['Overtime'].astype(float)
        detail['Other'] = detail['Other'].astype(float)
        detail['Project/Job Number'] = detail['Project/Job Number'].astype(str)
        detail['Sub Project / Job Number'] = detail['Sub Project / Job Number'].astype(str)
        detail['Job Cost Distribution'] = detail['Job Cost Distribution'].astype(str)
        detail['Week Ending Date'] = detail['Week Ending Date'].astype('datetime64')
        return detail


    def df(self):
        return self.detail()

    def export(self, output_name='export'):
        self.detail().to_excel(output_name, index=False, header=True)


directory = 'documentation'
dumps = [
    os.path.abspath(os.path.join(dirpath, f)) 
    for dirpath,_,file_names in os.walk(directory) 
    for f in file_names 
    if f.split('.')[1] == 'xlsx' 
   ]
files = [HCSSExport(d).detail() for d in dumps]

print(dumps)


# Flatten dataset to make sure typings are accurate

new_df = pd.concat(files)
# new_df = pd.concat(new_df, files[2].detail())
# new_df.to_excel('test.xlsx', index=False, header=True)
new_df.to_excel('test.xlsx')
print(new_df.head())
