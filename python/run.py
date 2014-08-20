import thread
from similarity import Similarity

# Create two threads as follows
try:
    sim = Similarity()
    folds = 4
    for i in range(1, folds+1):
        thread.start_new_thread( sim.run, (i, folds, ))
        
except:
   print "Error: unable to start thread"
   
print 'DONE'
