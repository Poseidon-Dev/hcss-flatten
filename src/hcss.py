import pandas as pd
import os

from src.ecmsconn import JobQuery

pd.options.display.float_format = '{:,.1f}'.format

path = os.getenv('PR_PATH')
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
            'Regular Hours': lambda x: float(x),
            'Overtime Hours': lambda x: float(x),
            'Other Hours': lambda x: float(x),
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
        self.df = pd.merge(self.df, states, how='left', on=['COMPANYNO', 'JOB', 'SUB'])
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
            # 'TTH',
            'TYPE',
        ]
        self.df = self.df[names]
        return self


    def change_to_date(self):
        self.df['WEEKENDING'] = pd.to_datetime(self.df['WEEKENDING']).dt.date
        return self


    def process(self):
        self.rename_df()
        self.hours_adjustments()
        self.add_states()
        self.convert_state_to_ukg()
        self.company_number_to_name()
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

    def collect_file_paths(self, sub_dir, directory=path):
        paths = [
            os.path.abspath(os.path.join(dirpath, f)) 
            for dirpath,_,file_names in os.walk(directory+sub_dir) 
            for f in file_names 
            if f.split('.')[1] == 'xlsx' 
        ]
        return paths


    @property
    def merge_heavy(self):
        frames = [HCSSExport(d).process() for d in self.collect_file_paths('/HEAVY')]
        return frames
   

    @property
    def merge_manual(self):
        frames = [HCSSExport(d).process() for d in self.collect_file_paths('/MANUAL')]
        return frames

    
    @property
    def merge(self):
        f1 = self.merge_heavy
        f2 = self.merge_manual
        df = pd.concat(f1+f2)
        return df


    def save(self, name='dumps/export.xlsx'):
        self.merge.to_excel(name, index=False, header=True)


class HourCalculations:

    def __init__(self, file_path=None):
        self.file_path = file_path
        self._df = MergeHeavy().merge


    @property
    def df(self):
        """
        Returns a dataframe that adds the column OTSTATE which contains
        the state in which an employees OT rules should follow
        COMPLETE
        """
        df = self._df.copy()
        multistate = self.multi_state_employees()
        df['OTSTATE'] = df.apply(lambda row: self.ot_state(row, multistate), axis=1)
        df['TYPE'] = df['TYPE'].fillna(value='')
        df.sort_values(by=['COMPANYNO', 'EMPLOYEENO', 'WEEKNO', 'DAYOFWEEK'])
        return df

    
    @property
    def ca_employees(self):
        """
        Returns the dataframe housing the compiled CA employee data
        """
        df = self.calc_ca_hours()
        df.drop(columns=['OT', 'OTSTATE'], inplace=True)
        return df


    @property
    def non_ca_employees(self):
        """
        Returns the dataframe housing the compiled non CA employee data
        """
        df = self.calc_non_ca_hours()
        df.drop(columns=['RUNNINGREG', 'HRS', 'OT', 'OTSTATE'], inplace=True)
        return df


    @property
    def all_employees(self):
        """
        Returns the df of all compiled data
        """
        return pd.concat([self.ca_employees, self.non_ca_employees])

    
    def save(self, path=path):
        data = self.all_employees
        date = self.all_employees.iloc[0]['WEEKENDING']
        date_string = f'{date.year}{date.month}{date.day}'
        name = f'{path}/{date_string}_merge.xlsx'
        data.to_excel(name, index=False, header=True)


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
        df['RUNNINGREG'] = df.groupby(['COMPANYNO', 'EMPLOYEENO', 'DEPT', 'WEEKENDING', 'WEEKNO'])['REG'].transform(pd.Series.cumsum)
        df['HRS'] = df.apply(lambda row: row['REG'] if row['RUNNINGREG'] <= 40 else row['REG'] - (row['RUNNINGREG'] - 40), axis=1)
        df['OT'] = df.apply(lambda row: row['REG'] - row['HRS'] + row['OVT'] if row['RUNNINGREG'] >= 40 else row['OVT'] , axis=1)
        df['OT'] = df.apply(lambda row: row['OT'] + row['HRS'] if row['HRS'] < 0 else row['OT'] , axis=1)
        df['HRS'] = df.apply(lambda row: 0 if row['HRS'] < 0 else row['HRS'], axis=1)

        df['REG'] = df['HRS']
        df['OVT'] = df['OT']

        return df

   
    def calc_ca_hours(self):
        """
        A factory method that runs appropriate static methods for
        each hours type for CA employes

        Need to look at compiling data for a given day
        COMPLETE
        """
        df = self.transpose_hours()

        return df


    def transpose_hours(self):
        """
        Moves over the additional hours if the total hours within 
        a day surpass the 8 hour maximum 
        """
        df = self.df[self.df['OTSTATE'] == 'CA'].copy()

        df['CARUNNINGREG'] = df.groupby(['COMPANYNO', 'EMPLOYEENO', 'DEPT', 'WEEKENDING', 'WEEKNO', 'DAYOFWEEK'])['REG'].transform(pd.Series.cumsum)
        df['CAPREVREG'] = df['REG'].shift()
        df['OT'] = df.apply(lambda row: row['OVT'] if row['CARUNNINGREG'] <= 8 else row['CARUNNINGREG'] - 8 + row['OVT'], axis=1)
        df['HR'] = df.apply(lambda row: row['REG'] if row['CARUNNINGREG'] <= 8 else 8 - row['CAPREVREG'], axis=1)
        df['DT'] = df.apply(lambda row: row['OTH'] if row['OT'] <= 4 else row['OT'] - 4 + row['OTH'] , axis=1)
        df['OT'] = df.apply(lambda row: row['OT'] if row['OT'] <=4 else row['OT'] - row['DT'], axis=1)
        df['CTYPE'] = df.apply(lambda row: row['TYPE'] if row['DT'] == 0 else 'DT', axis=1)

        df['REG'] = df['HR']
        df['OVT'] = df['OT']
        df['OTH'] = df['DT']
        df['TYPE'] = df['CTYPE']

        df.drop(columns=['CARUNNINGREG', 'CAPREVREG', 'HR', 'DT', 'CTYPE'], inplace=True)
      
        return df


    def check_days_worked(self):
        """
        Returns a list of employees who worked 7 days within a week
        within CA
        """
        df = self.df[self.df['OTSTATE'] == 'CA'].copy()
        cols = ['COMPANYNO', 'EMPLOYEENO', 'WEEKNO', 'DAYOFWEEK']
        df = df[cols].groupby(['COMPANYNO', 'EMPLOYEENO', 'WEEKNO'])['DAYOFWEEK'].nunique().reset_index()
        df = df[df['DAYOFWEEK'] >= 7]
        return df['EMPLOYEENO'].tolist()


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
        """
        Booleanizes issued state if CAHQ
        """
        if row['STATE'] == 'CAHQ':
            return 1
        else:
            return 0

    