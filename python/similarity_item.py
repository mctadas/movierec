#!/usr/bin/python
# -*- coding: utf-8 -*-
 
import os, sys
import pyodbc
import time

class Similarity(object):

    def __init__(self):
        self.ui_matrix = {}
        self.indexes = {}
        self.conn = self.getDbConnection()
        self.nn_count = 100

        self.part = None
        self.parts = None

    def __del__(self):
        self.conn.close()

    def run(self, part=1, parts=1):
        self.part = part
        self.parts = parts
        
        self.loadData()

        self.computeU2USimilarity()
        
    def computeU2USimilarity(self):
        all_users = self.ui_matrix.keys()
        print 'Computing similarity for ',len(all_users), "items in ui_mtrix.."
    
        # compute only for part of users (for parallel computing)
        step_size = int(len(all_users) / self.parts)
        self.start = ((self.part-1)*step_size)+1
        self.finish = (self.part*step_size)
        all_users.sort()
        users = all_users[self.start:self.finish]

        for i, u1 in enumerate(users):
            print ' processsing user %d' % (i+self.start)
            sim = []
            for u2 in all_users:
                if u1 == u2: continue
                correlation = self.getCommonItemsCorrelation(self.ui_matrix[u1], self.ui_matrix[u2])        
                if correlation > 0:
                    sim.append((correlation, u2))

            topNNeighbours = self.getTopNNeighbours(sim, self.nn_count)
            self.saveSimilaritiesToDB(u1, topNNeighbours)

    def getCommonItemsCorrelation(self, set1, set2):
        a = len(set1.intersection(set2))
        common_items = set1.union(set2)
        b = len(common_items)
        
        try:
            correlation = a / float(b)
            return correlation
        except:
            return -1
    
    def getTopNNeighbours(self, sim, n):
        tmp_list = sorted(sim, reverse=True)
        tmp_list = tmp_list[:n]
        return tmp_list
    
    def saveSimilaritiesToDB(self, u1, u2_sim):
        print 'store new similarity'

        try:
            cursor = self.conn.cursor()

            sql_del = "DELETE FROM [DWH_Darbinis].[dbo].[IPTV_ITEM_similarities] WHERE [UID] = %d " % (u1)
            cursor.execute(sql_del)

            for i in u2_sim:
                sql_ins = "INSERT INTO [DWH_Darbinis].[dbo].[IPTV_ITEM_similarities] VALUES (%d, %d, %f)" % (u1, i[1], i[0])
                cursor.execute(sql_ins)
            self.conn.commit()
        except:
            print 'reconnecting to DB..'
            time.sleep(10)
            self.conn.close()
            self.conn = self.getDbConnection()
            saveSimilaritiesToDB(u1, u2_sim)
    
    def loadData(self):
        q1 = '''select tv_id as UID,
        [main_id]
      ,[chan_id]
      ,[title]
      ,[description]
      ,[image_name]
      ,[prodyear]
      ,[category]
      ,[series_id]
      ,[genres]
      ,[episode]
      ,[real_chan_id]
      ,[prad_laikas]
      from [DWH_Darbinis].[dbo].[VOD Duomenys ext]
        '''
        self.loadDataFromSql(q1)

        q2 = '''select [tv_id] as UID,
        s.name as IID
        from [DWH_Darbinis].[dbo].[VOD Duomenys ext], [DWH_Darbinis].[dbo].[VOD features] s
        WHERE [start_date] >= DATEADD(day, -1, GETDATE())
                   AND [tv_id] IS NOT NULL
				   group by [tv_id], s.name'''
        #self.loadDataFromSql(q2)

    def getDbConnection(self):
        return pyodbc.connect(r'DRIVER={SQL Server};Server=SRDWH\CAVS;Trusted_Connection=yes;')

    def loadDataFromSql(self, sql):
        print sql
        conn = self.getDbConnection()
        cursor = conn.cursor()
        
        cursor.execute(sql)
        c = 0
        while True:
            row = cursor.fetchone()
            if not row: break
            user = row[0]
            for i, value in enumerate(row[1:]):
                key = "%d__%s" % (i, value)
                index = self.getIndex(key)
                self.addToDictSet(user, index)
            if c == 2: break
            c += 1
        conn.close()
        
    def getIndex(self, str):
        try:
            return self.indexes[str]
        except:
            index = len(self.indexes)
            self.indexes[str] = index
            return index

     
    def addToDictSet(self, key, value):
        key = int(key)
        value = int(value)
        dictionary = self.ui_matrix
        
        if key in dictionary:
            dictionary[key].add(value)
        else:
            dictionary[key] = set([value])
        return dictionary

def main():
    part = 1 # int(sys.argv[1])
    parts = 1 # int(sys.argv[2])
    sim = Similarity()
    sim.run(part, parts)


if __name__ == "__main__":
    sys.exit(main())
