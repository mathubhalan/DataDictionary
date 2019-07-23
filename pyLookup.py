# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 13:57:55 2019
python pyLookup.py --excel="C:/Users/mathu_gopalan/Documents/Bofa/sheet1.xlsx" --sql="C:\\Users\\mathu_gopalan\\Documents\\Bofa\\"

@author: Mathu_Gopalan
"""
import pandas as pd
import glob, argparse

class pyLookup:
    def __init__(self, fpath, fdir):
        self.fpath=fpath
        self.fdir=fdir
        
    
    def readExcel(self):
        '''
        Read excel file and returns back as transformed dataframe
        '''
        df_excel = pd.read_excel(self.fpath)
        df_excel = df_excel.apply(lambda x:x.astype(str).str.lower())
        df_excel["tablename"] = df_excel["OWNER"].str.strip()+"."+df_excel["TABLE_NAME"].str.strip()+"."+df_excel["COLUMN_NAME"].str.strip()
        
        return df_excel
    
    def readFile(self):
        '''
        Read the sql files as list and returns a dataframe of raw content
        '''
        files = [f for f in glob.glob(self.fdir + "**/*.sql", recursive=True)]        
        content_schema=[]
        for f in files:
            with open (f, 'rt', encoding="utf8") as myfile:
                contents = myfile.read() 
            content_temp = contents.split(";")
            content_list = [x.replace('\n', ' ') for x in content_temp]
            temp_df=pd.DataFrame(content_list)
            #temp_df1=temp_df.T
            content_schema.append(temp_df)
            print(f"\n------Loaded the data from {f} file------")
        
        frame = pd.concat(content_schema,axis=0,ignore_index=True)
        frame.rename(columns={frame.columns[0]:"schema"}, inplace=True)
        frame.dropna(axis=0, inplace=True)
    
        return frame
      
        '''  
    def readSQL(self):
        '''
        #Read the sql file and returns the transformed dataframe
        '''
        files = [f for f in glob.glob(self.fdir + "**/*.sql", recursive=True)]        
        li=[]
        for f in files:            
            
            df_t=pd.read_csv(f, engine="python", index_col=False, header=None, delimiter='\;')
            df_t1=df_t.T
            li.append(df_t1)
            print(f"\n------Loaded the data from {f} file------")
        frame = pd.concat(li, axis=0, ignore_index=True)
        frame.rename(columns={frame.columns[0]:"schema"}, inplace=True)
        frame.dropna(axis=0, inplace=True)
        
        return frame'''
    
    def data_process(self, df_excel, df_sql):
        '''
        Read the two dataframe, create new variable and merge the df based on tablename
        '''
        print("\n------Data cleansing in SQL dataframe------")
        new = df_sql["schema"].str.split("IS", n=1, expand=True)
        df_sql["tabledata"]=new[0]
        df_sql["commentdata"]=new[1]
        tablename=df_sql["tabledata"].str.split("ON COLUMN", n=1, expand=True)
        print("\n------Created the tablename column in SQL dataframe------")
        df_sql["tablename"]=tablename[1].str.strip()
        #df_sql["tablename"]=df_sql["tablename"].str.strip()
        print("\n------Data Merge in progress-------")
        df_excel = pd.merge(df_excel, df_sql[["commentdata","tablename"]], on="tablename", how="left")       
        #df_excel = pd.merge(df_excel, df_sql, on="tablename", how="left")
        
        return df_excel


if __name__ =='__main__':
    parser = argparse.ArgumentParser(description="Python based lookup for the data dictionary")
    parser.add_argument('--excel', dest="fpath", action="store", type=str, \
                        help = "Enter the Excel file path ex. C:/folder/excel.excel")
    parser.add_argument('--sql', dest="fdir", action = "store", type=str, \
                        help = "Enter the Folder lolcation of the SQL files ex. C/folder/")
    pa = parser.parse_args()
    fpath=pa.fpath
    fdir=pa.fdir
    print("\n------Initilized the task runner------")
    lc = pyLookup(fpath, fdir)
    print("\n------Reading the excel file and loading in dataframe------")
    df_e=lc.readExcel()
    print("\n------Loaded Excel data in a dataframe, sample data------")
    print(df_e.head())
    print("\n------Reading the SQL files and loading in dataframe------")
    df_sql = lc.readFile()
    print("\n------SQL files are loaded into a dataframe, sample data------")
    print(df_sql.head())
    df_process = lc.data_process(df_e,df_sql)
    print("\n------Data Merge completed------")
    print(df_process.head())
    writepath = fdir+"processdata.xlsx"
    df_process.to_excel(writepath)
    print(f"\n \n Task completed, processed file is stored in {writepath} !!")
    
    
    
    


