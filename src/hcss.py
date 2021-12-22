import pandas as pd
import os
from src.ecmsconn import JobQuery

class HCSSExport:

    def __init__(self, file_path):
        self.file_path = file_path
        self.cols = [
            'Company Number',
            'Employee Number',
            'Week Number',
            'Day of Week',
            'Project/Job Number',
            'Sub Project / Job Number',
            'Job Cost Distribution',
            'Regular Hours',
            'Overtime Hours',
            'Other Hours',
            'Other Hours Type',
            'Department Number',
            'Week Ending Date',
            ]
        self.grouping = [
            'COMPANYNO',
            'EMPLOYEENO',
            'WEEKNO', 
            'DAYOFWEEK',
            'JOB',
            'SUB',
            'JCDIST',
            'DEPT',
            'WEEKENDING',
            'TYPE',
        ]
        self.safe_names = {
            'Project/Job Number': 'JOB',
            'Sub Project / Job Number': 'SUB'
        }
        self.df = pd.read_excel(self.file_path, converters={
            'Sub Project / Job Number': lambda x: str(x),
            'Job Cost Distribution': lambda x: str(x),
            })[self.cols]

    
    def rename_df(self):
        names = [
            'COMPANYNO',
            'EMPLOYEENO',
            'WEEKNO', 
            'DAYOFWEEK',
            'JOB',
            'SUB',
            'JCDIST',
            'REG',
            'OVT',
            'OTH',
            'TYPE',
            'DEPT',
            'WEEKENDING'
        ]
        self.df.columns = names
        return self


    def company_number_to_name(self):
        companies = {1: 'APC', 30: 'MEE', 40: 'GCS' }
        self.df['COMPANYNO'] = self.df['COMPANYNO'].replace(companies)
        return self


    def hours_adjustments(self):
        self.df = self.df.groupby(self.grouping, group_keys=True, dropna=False).agg(
            REG=pd.NamedAgg(column='REG', aggfunc='sum'),
            OVT=pd.NamedAgg(column='OVT', aggfunc='sum'),
            OTH=pd.NamedAgg(column='OTH', aggfunc='sum'),
        ).reset_index()
        self.df['TTH'] = self.df['REG'] + self.df['OVT'] + self.df['OTH']
        self.df['REG'] = self.df['REG'].astype(float)
        self.df['OVT'] = self.df['OVT'].astype(float)
        self.df['OTH'] = self.df['OTH'].astype(float)
        return self

    
    def fetch_state(self):
        self.df['State'] = self.df.apply(lambda x: JobQuery(x['Project/Job Number'], x['Sub Project / Job Number']).to_df()['STATE'], axis=1)
        return self


    def zfill_subjob(self):
        self.df['SUB'] = self.df['SUB'].str.zfill(3)
        return self


    def job_merge(self):
        self.df['PROJECT'] = self.df['JOB'].astype(str) + self.df['SUB']
        self.df.drop(columns='JOB', axis=1, inplace=True)
        self.df.drop(columns='SUB', axis=1, inplace=True)
        return self


    def phase_code_split(self):
        self.df['JCDIST1'] = self.df['JCDIST'].str[:6]
        self.df['JCDIST2'] = self.df['JCDIST'].str[6:]
        self.df.drop(columns='JCDIST', axis=1, inplace=True)
        return self


    def reorder_df(self):
        names = [
            'COMPANYNO',
            'EMPLOYEENO',
            'DEPT',
            'WEEKENDING',
            'WEEKNO', 
            'DAYOFWEEK',
            'PROJECT',
            'JCDIST1',
            'JCDIST2',
            'REG',
            'OVT',
            'OTH',
            'TTH',
            'TYPE',
        ]
        self.df = self.df[names]
        return self


    def change_to_date(self):
        self.df['WEEKENDING'] = pd.to_datetime(self.df['WEEKENDING']).dt.date
        return self


    def process(self):
        self.rename_df()
        self.company_number_to_name()
        self.hours_adjustments()
        self.zfill_subjob()
        self.job_merge()
        self.phase_code_split()
        self.reorder_df()
        self.change_to_date()
        self.df.fillna('', inplace=True)
        return self.df


    def export(self, output_name='dumps/export.xlsx'):
        try:
            self.process().to_excel(output_name, index=False, header=True)
        except Exception as e:
            print(e)
            return False


class MergeHeavy:

    def collect_file_paths(self, directory='documentation'):
        paths = [
            os.path.abspath(os.path.join(dirpath, f)) 
            for dirpath,_,file_names in os.walk(directory) 
            for f in file_names 
            if f.split('.')[1] == 'xlsx' 
        ]
        return paths


    @property
    def merge(self):
        frames = [HCSSExport(d).company_number_to_name() for d in self.collect_file_paths()]
        df = pd.concat(frames)
        return df
        df = add_state(df)

    
    def add_state(self, df):
        df['State'] = df.apply(lambda x: JobQuery(x['Project/Job Number'], x['Sub Project / Job Number']).to_df()['STATE'], axis=1)
        return df


    def save(self, name='export.xlsx'):
        self.merge.to_excel(name, index=False, header=True)