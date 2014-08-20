#!/usr/bin/python
# -*- coding: utf-8 -*-
 
import os, sys
import pyodbc
from time import gmtime, strftime

def main():
#    if len(sys.argv) < 2:
#        sys.exit('Usage: %s {vod, tv}' % sys.argv[0])
#    
    rec = Recommender()
    rec.runVOD()

class Recommender(object):
    def __init__(self, n_predictions = 50, n_init_n_count = 20):
        self.ui_matrix = {}
        self.neighbours = {}
        self.indexes = {}
        self.dest = '[EPDM].[dbo].[IPTV_VOD_Predictions]'
        
        self.ui_from = '20130101'
        self.prediction_count = n_predictions
        self.neighbours_init_count = n_init_n_count

        self.conn = self.getDbConnection()

        # FIFO stack for breadth first neighbour traverse
        self.stack = []
        self.stack_cursor = 0
        
    def __del__(self):
        self.conn.close()

    def getDbConnection(self):
        return pyodbc.connect(r'DRIVER={SQL Server};Server=SRDWH\CAVS;Trusted_Connection=yes;')

    def runVOD(self):
        self.loadDataFromFile("C:/Users/tadmkc/workspace/Recommender/data/train/vod_rated.csv")
        self.loadNeighboursFromFile("C:/Users/tadmkc/workspace/Recommender/data/train/vod_sim.csv")
        self.computeCollaborativeFilteringPredictions("C:/Users/tadmkc/workspace/Recommender/data/pred/vod_py_pred.csv")

    def runChannel(self):
        self.loadChannelData()
        self.loadNeighbours()
        self.computeCollaborativeFilteringPredictions('CHANNEL.csv')

    def loadNeighboursFromFile(self, filename):
        print "neighbours"
        f = open(filename, 'r')
        for row in f:
            row = row.split(',')[:3]
            self.addToDictList(int(row[0]), (float(row[2]), int(row[1])), self.neighbours)
        f.close()

    def loadNeighbours(self):
        q = '''-- Similarities --
                SELECT UID, NEIGHBOUR_ID, SIMILARITY
                  FROM EPDM.dbo.IPTV_USER_similarities
                 ORDER BY UID, SIMILARITY desc'''
        print
        cursor = self.conn.cursor()
        cursor.execute(q)
        while True:
            row = cursor.fetchone()
            if not row: break
            self.addToDictList(int(row[0]), (float(row[2]), int(row[1])), self.neighbours)

    def loadChannelData(self):
        q2 = '''-- Wathed TV
                SELECT account_no AS UID,
                       prog_id AS IID
                    FROM Ataskaitoms.Gala_metras.atrintos_TVPerziuros_ir_TVIrasuPerziuros t1
                    LEFT JOIN
                    [Ataskaitoms].[dbo].[GALA_metras_TV_Programme] t2
                    ON  t1.KanaloID = t2.channel_ID and t1.Laida = t2.tv_title
                    WHERE pradzia >= DATEADD(day, -3, GETDATE())
                      AND [Minuciu skaicius] > 7
                      AND account_no IS NOT NULL'''
        self.loadDataFromSql(q2)            

    def loadData(self):
        q1 = '''-- Video On Demand --
                SELECT ACCOUNT_NO as UID,
                       a.FILMO_ID as IID
                  FROM EPDM.dbo.GALA_VOD a
                       JOIN
                       EPDM.dbo.VOD as b
                       ON a.FILMO_ID = b.Filmo_id
                 WHERE EVENTTIME > '%s'
                   AND b.Filmo_uzdarymo_data > GETDATE()
                 ORDER BY UID''' % self.ui_from
        self.loadDataFromSql(q1)
        
    def getIndex(self, str):
        str = str.encode('ascii', 'ignore')
        try:
            return self.indexes[str]
        except:
            index = len(self.indexes)
            self.indexes[str] = index
            return index
        
       
    def loadDataFromSql(self, sql):
        print sql
        cursor = self.conn.cursor()
        
        cursor.execute(sql)
        while True:
            row = cursor.fetchone()
            if not row: break
            self.loadRow(row)

    def loadDataFromFile(self, filename):
        print "data"
        f = open(filename, 'r')
        for row in f:
            self.loadRow(row.split(',')[:3])
        f.close()
                
    def loadRow(self, row):
        user = row[0]
        for i, value in enumerate(row[1:]):
            key = "%d__%s" % (i, value)
            index = self.getIndex(key)
            self.addToDictSet(user, index)

    def addToDictSet(self, key, value, dictionary = None):
        if dictionary is None: dictionary = self.ui_matrix
        key = int(key)
        value = int(value)
        
        if key in dictionary:
            dictionary[key].add(value)
        else:
            dictionary[key] = set([value])
        return dictionary

    def addToDictList(self, key, value, dictionary = None):
        if dictionary is None: dictionary = self.ui_matrix
        
        if key in dictionary:
            dictionary[key].append(value)
        else:
            dictionary[key] = [value]
        return dictionary
    
    def computeCollaborativeFilteringPredictions(self, res_filename):
        print 'compute prediction..'
        all_users = self.neighbours.keys()
        print len(all_users), "users in ui_matrix"
        all_users.sort()
        users = all_users#[start:finish]

        for i, u1 in enumerate(users):
            #print strftime("%Y-%m-%d %H:%M:%S", gmtime()), ' processsing user %d' % (i)#+start)
            self.stack = [u1]
            self.stack_cursor = 0
            pred = self.computePredictions({})
            
            recommendations = self.aggregatePredictionValue(pred)
            #self.insertIntoPredictionDatabase(recommendations, u1)
            self.writeToFile(recommendations, u1, res_filename)
            
    def addUserNode(self, u):
        try:
            self.stack += [n for _, n in self.neighbours[u] if n not in self.stack]
        except KeyError:
            pass
        
    def computePredictions(self, pred):
        while True:
            u = self.stack[self.stack_cursor]
            self.addUserNode(u)

            try:
                pred = self.computeTopNPredictions(u, 0, self.neighbours_init_count, self.neighbours[u], pred)
            except KeyError:
                pass
            
            self.stack_cursor += 1
            if self.stack_cursor >= len(self.stack) or len(pred) >= self.prediction_count:
                break
       
        return pred
        
    def aggregatePredictionValue(self, pred):
        # aggregate item similarities
        aggr_pred = []
        for i in pred:
            aggr_pred.append((sum(pred[i]), i))

        # sort dictionary by value descending
        aggr_pred.sort(reverse=True)

        return aggr_pred[:self.prediction_count]
        
    def computeTopNPredictions(self, u, nn_from, nn_to, neighbours, pred):
        if len(neighbours) < nn_to: return pred
            
        nn_top_n = self.getNeighbours(neighbours, 0, nn_from)
        pred = self.getTopNRecommendations(u, nn_top_n, pred)

        # recursion if neighbour size is insuficient
        if len(pred) < self.prediction_count:
            pred = self.computeTopNPredictions(u, nn_to, nn_to+20, neighbours, pred)

        return pred

    def getTopNRecommendations(self, u, nn, pred):
        for sim, neighbour in nn:
            if neighbour in self.stack[:self.stack_cursor]:
                continue # do not process same users multiple times
            try:
                items = self.ui_matrix[neighbour].difference(self.ui_matrix[u])
                for i in items:
                    pred = self.addToDictList(i, sim, pred)
            except KeyError:
                pass #neighbour did not watch any movie
        return pred

    def getNeighbours(self, sim, n_from, n_to):
        tmp_list = sorted(sim, reverse=True)
        tmp_list = tmp_list[n_from:n_to]
        return tmp_list

    def writeToFile(self, pred, u, res_filename):
        f = open(res_filename, 'a')
        
        for p in pred:
            tmpStr = "%d,%d,%f\n" % (u, p[1], p[1])
            f.write(tmpStr)
            
        f.close()
        
    def insertIntoPredictionDatabase(self, pred, u):
        cursor = self.conn.cursor()
        t = strftime("%Y-%m-%d", gmtime())
        
        for i, p in enumerate(pred):
            item = self.getDictKey(p[1], self.indexes)
            sql = "INSERT INTO %s VALUES (%d, %d, %d, '%s')" % (self.dest, u, int(item), i, t)
            cursor.execute(sql)
            
        self.conn.commit()
        
    def getDictKey(self, value, dictionary):
        for key, val in dictionary.iteritems():
            if val == value:
                return key.split('__')[1]

if __name__ == "__main__":
    sys.exit(main())
