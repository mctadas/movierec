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
        
        #self.loadData()
        self.loadDataFromFile("C:/Users/tadmkc/workspace/Recommender/data/train/vod_rated.csv")
        self.computeU2USimilarity()
        
    def computeU2USimilarity(self):
        all_users = self.ui_matrix.keys()
        print 'Computing similarity for ',len(all_users), "users in ui_mtrix.."

        # compute only for part of users (for parallel computing)
        step_size = int(len(all_users) / self.parts)
        self.start = ((self.part-1)*step_size)+1
        self.finish = (self.part*step_size)
        all_users.sort()
        users = all_users[self.start:self.finish]


        f = open("C:/Users/tadmkc/workspace/Recommender/data/train/vod_sim.csv", 'a')

        for i, u1 in enumerate(users):
            print ' processsing user %d' % (i+self.start)
            sim = []
            for u2 in all_users:
                if u1 == u2: continue
                correlation = self.getCommonItemsCorrelation(self.ui_matrix[u1], self.ui_matrix[u2])        
                if correlation > 0:
                    sim.append((correlation, u2))

            topNNeighbours = self.getTopNNeighbours(sim, self.nn_count)
            #self.saveSimilaritiesToDB(u1, topNNeighbours)
            self.saveSimilaritiesToFile(u1, topNNeighbours, f)

        f.close();

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

    def saveSimilaritiesToFile(self, u1, u2_sim, f):
        for i in u2_sim:
            row = "%d,%d,%f\n" %(u1, i[1], i[0])
            f.write(row)
    
    def saveSimilaritiesToDB(self, u1, u2_sim):
        exit
        print 'store new similarity'

        try:
            cursor = self.conn.cursor()

            sql_del = "DELETE FROM [EPDM].[dbo].[IPTV_USER_similarities] WHERE [UID] = %d " % (u1)
            cursor.execute(sql_del)

            for i in u2_sim:
                sql_ins = "INSERT INTO [EPDM].[dbo].[IPTV_USER_similarities] VALUES (%d, %d, %f)" % (u1, i[1], i[0])
                cursor.execute(sql_ins)
            self.conn.commit()
        except:
            print 'reconnecting to DB..'
            time.sleep(10)
            self.conn.close()
            self.conn = self.getDbConnection()
            saveSimilaritiesToDB(u1, u2_sim)
    
    def loadData(self):
        q1 = '''-- Video On Demand --
                SELECT ACCOUNT_NO as UID,
                       FILMO_ID as IID
                  FROM EPDM.dbo.GALA_VOD'''
        self.loadDataFromSql(q1)

        q2 = '''-- Wathed TV
                SELECT [account_no] as UID,
                        CAST([KanaloID] AS varchar) + ' '+ [Laida] as IID
                  FROM [Ataskaitoms].[Gala_metras].[atrintos_TVPerziuros_ir_TVIrasuPerziuros]
                 WHERE [pradzia] >= DATEADD(day, -30, GETDATE())
                   AND [account_no] IS NOT NULL
                 GROUP BY [account_no], CAST([KanaloID] AS varchar) + ' '+ [Laida]'''
        self.loadDataFromSql(q2)

        q3 = '''-- Metadata on user --
                SELECT pg.PG_nr
                        ,pg.ArVip
                        ,pg.galima_naudoti_rinkodaros_tikslams
                        ,pg.segmentas
                        ,pg.senjoras
                        ,pg.skolininkas
                        ,pg.tiesioginis_debetas
                        ,CONVERT(int, ROUND(DATEDIFF(hour,pg.gimimo_data,GETDATE())/8766, -1))
                        ,pg.Saskaitos_formatas
                        ,pg.mano_teo
                        ,pg.lytis
                   FROM EPDM.dbo.SW_INST_PRODUCT sip
                        JOIN
                        RINKODAROS_DB_KPI.dbo.PG_dim pg
                        ON sip.account_no = pg.PG_nr
                  WHERE pg.Valid_to = '9999-12-31'
                    AND pg.PG_nr is NOT NULL
                    AND sip.swProdReleaseId IN (84, 205)
                    AND (sip.ltStopDate IS NULL
                         OR sip.ltStopDate > GETDATE())'''
        self.loadDataFromSql(q3)

    def getDbConnection(self):
        return pyodbc.connect(r'DRIVER={SQL Server};Server=SRDWH\CAVS;Trusted_Connection=yes;')

    def loadDataFromSql(self, sql):
        print sql
        conn = self.getDbConnection()
        cursor = conn.cursor()
        
        cursor.execute(sql)
        while True:
            row = cursor.fetchone()
            if not row: break
            loadRow(row)
            
        conn.close()

    def loadDataFromFile(self, filename):
        f = open(filename, 'r')
        for row in f:
            self.loadRow(row.split(',')[:2])
        f.close()

    def loadRow(self, row):
        user = row[0]
        for i, value in enumerate(row[1:]):
            key = "%d__%s" % (i, value)
            index = self.getIndex(key)
            self.addToDictSet(user, index)
        
       
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
    part = 1#00#int(sys.argv[1])
    parts = 1#00#int(sys.argv[2])
    sim = Similarity()
    sim.run(part, parts)


if __name__ == "__main__":
    sys.exit(main())
