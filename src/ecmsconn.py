import os
import pyodbc
import pandas as pd

class EcmsQuery:

    command = ''

    def __init__(self):
        self.ERP_HOST = os.getenv('ERP_HOST')
        self.ERP_UID = os.getenv('ERP_UID')
        self.ERP_PWD = os.getenv('ERP_PWD')


    def conn(self):
        return pyodbc.connect(f'DSN={self.ERP_HOST}; UID={self.ERP_UID}; PWD={self.ERP_PWD}')

    
    def to_df(self):
        return pd.read_sql(self.command, self.conn())


class JobQuery(EcmsQuery):

    command = """
        SELECT COMPANYNUMBER as COMPANYNO, JOBNUMBER as JOB, trim(SUBJOBNUMBER) as SUB, STATECODE as State 
        FROM CMSFIL.JCTDSC 
        WHERE COMPANYNUMBER in (1, 30, 40)
        ORDER BY JCTDSCID DESC
        """
