import pandas as pd
import os

from src.ecmsconn import JobQuery

pd.options.display.float_format = '{:,.0f}'.format

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
            'Project/Job Number': lambda x: str(x),
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
        self.df['STATE'] = self.df.apply(lambda x: JobQuery(x['JOB'], x['SUB']).to_df()['STATE'], axis=1)
        return self


    def grab_states(self):
        states = JobQuery().to_df()
        states['STATE'] = states['STATE'].astype(int)
        return states
    

    def add_states(self):
        states = self.grab_states()
        self.df['SUB'] = self.df['SUB'].fillna('')
        self.df['SUB'] = self.df['SUB'].astype(str)
        self.df = pd.merge(self.df, states, how='left', on=['JOB', 'SUB'])
        return self

    
    def convert_state_to_ukg(self):
        converter = {
            30: 'AZ',
            31: 'AZ',
            50: 'CAHQ',
            320: 'NM',
            290: 'NV',
            380: 'OR',
            631: 'AZ',
            650: 'OR',
        }
        self.df['STATE'] = self.df['STATE'].replace(converter)
        return self


    def job_merge(self):
        self.df['PROJECT'] = self.df['JOB'].astype(str) + self.df['SUB']
        self.df.drop(columns='JOB', axis=1, inplace=True)
        self.df.drop(columns='SUB', axis=1, inplace=True)
        return self

    
    def zfill_subjob(self):
        self.df['SUB'] = self.df['SUB'].apply(lambda x: x.zfill(3) if len(x) > 0 else '')
        # self.df['SUB'] = self.df['SUB'].str.zfill(3)
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
            'STATE',
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
        self.add_states()
        self.convert_state_to_ukg()
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
        frames = [HCSSExport(d).process() for d in self.collect_file_paths()]
        df = pd.concat(frames)
        return df
   

    def save(self, name='dumps/export.xlsx'):
        self.merge.to_excel(name, index=False, header=True)


class HourCalculations:

    def __init__(self, file_path):
        self.file_path = file_path
        self._df = pd.read_excel(self.file_path)


    @property
    def df(self):
        """
        Returns a dataframe that adds the column OTSTATE which contains
        the state in which an employees OT rules should follow
        COMPLETE
        """
        df = self._df.copy()
        df = df[df.EMPLOYEENO == 10027]
        multistate = self.multi_state_employees()
        df['OTSTATE'] = df.apply(lambda row: self.ot_state(row, multistate), axis=1)
        return df


    def multi_state_employees(self):
        """
        Returns a list employee numbers who worked in two states with one state being CA
        COMPLETE
        """
        ## Copy Original Dataframe
        multi_state_df = self._df.copy()

        ## Check if the employee worked in california
        multi_state_df['STATEBOOL'] = multi_state_df.apply(lambda row: self.check_state(row), axis=1)

        ## Group the dataframe to return singluar rows per state entry
        multi_state_df = multi_state_df.groupby(['COMPANYNO', 'EMPLOYEENO', 'DEPT', 'WEEKENDING', 'WEEKNO', 'STATEBOOL']).size().reset_index()
        
        ## Return a dataframe that contains the employees who worked in two states, with one being CA
        multi_state_df = multi_state_df['EMPLOYEENO'].value_counts().reset_index()
        multi_state_df = multi_state_df[multi_state_df['EMPLOYEENO'] > 1]
        
        return multi_state_df['index'].tolist()

    
    def calc_non_ca_hours(self):
        """
        Returns a list of employees who worked not in CA and over 40 hours
        COMPLETE
        """
        df = self.df[self.df['OTSTATE'] != 'CA'].copy()
        df['RUNNINGHRS'] = df['REG'].cumsum(axis=0)

        for idx,row in df.iterrows():
            if row['RUNNINGHRS'] <= 40:
                df.loc[idx, 'HRS'] = row['REG']
            else:
                df.loc[idx, 'HRS'] = 0
                df.loc[idx, 'OT'] = row['REG']

        df.drop(columns='RUNNINGHRS', axis=1, inplace=True)
        df.fillna(0, inplace=True)
        df['OTHER'] = df['OTH']
        df['OTTYPE'] = df['TYPE']
        return df

   
    def calc_ca_hours(self):
        """
        A factory method that runs appropriate static methods for
        each hours type for CA employes

        Need to look at compiling data for a given day
        """
        df = self.transpose_hours_in_days()

        df['HRS'] = 0
        df['OT'] = 0
        df['OTHER'] = 0
        df['OTTYPE'] = ''

        for idx,row in df.iterrows():
            self.regular_hours_transpose(idx, row, df)
            self.overtime_hours_transpose(idx, row, df)
            self.other_hours_transpose(idx, row, df)
       
        return df


    def transpose_hours_in_days(self):
        """
        Moves over the additional hours if the total hours within 
        a day surpass the 8 hour maximum 
        """
        df = self.df[self.df['OTSTATE'] == 'CA'].copy()
        print(df)
        print('\n')
        day_of_week = 1
        reg_counter = 0
        ovt_counter = 0
        oth_counter = 0
        for idx, row in df.iterrows():
            if row['DAYOFWEEK'] == day_of_week:
                reg_counter += row['REG']
                ovt_counter += row['OVT']
                oth_counter += row['OTH']
                if reg_counter > 8:
                    df.loc[idx, 'OVT'] = df.loc[idx, 'REG'] + df.loc[idx-1, 'REG'] - 8
                    df.loc[idx, 'REG'] = 8 - df.loc[idx-1, 'REG']
            else:
                day_of_week = row['DAYOFWEEK']
        return df


    @staticmethod
    def regular_hours_transpose(idx, row, df):
        """
        Max regular hours to 8 
        """
        if row['REG'] >= 8:
            df.loc[idx, 'HRS'] = 8
        else:
            df.loc[idx, 'HRS'] = row['REG']

    @staticmethod
    def overtime_hours_transpose(idx, row, df):
        """
        Take any hours over 8 in a day, and make them overtime
        """
        df.loc[idx, 'OT'] = row['OVT'] + (row['REG'] - df.loc[idx, 'HRS'])

    
    @staticmethod
    def other_hours_transpose(idx, row, df):
        """
        If overtime hours are over 4 hours, then move difference to otherhours and 
        change other type to DT
        """
        if row['OTH'] + (df.loc[idx, 'OT'] - 4) > 0:
            df.loc[idx, 'OTHER'] = row['OTH'] + (df.loc[idx, 'OT'] - 4)
            df.loc[idx, 'OTTYPE'] = 'DT'
            df.loc[idx, 'OT'] = df.loc[idx, 'OT'] - df.loc[idx, 'OTHER']
            

    @staticmethod
    def ot_state(row, multistate):
        """
        Checks to see if the employee has worked in multiple states
        If they worked at least in CA, then return CA
        Else return their respective working state
        """
        if row['EMPLOYEENO'] in multistate:
            return 'CA'
        elif row['STATE'] == 'CAHQ':
            return 'CA'
        else:
            return row['STATE']


    @staticmethod
    def check_state(row):
        if row['STATE'] == 'CAHQ':
            return 1
        else:
            return 0

    