import csv
import time
import sys


# python3 evoker.py "./test_dataset.csv"


# parameters
# ============================================
#TarHist = sys.argv[1]   # target history file

ProcessTimeDelay = 2    # seconds
Windowsize = 5  # number of records skipped
                # assuming windowsize delay only apply for "0" signal after a split takes place.
                # i.e. after a split take place, algorithm can directly split again but can only merge after leaving window bound
                # merge won't cause window delay. i.e. after a merge takes place, algorithm can directly merge or split based on signal.
                # once left the window, can do either split or merge.
# ============================================

def stream_annotate(filename,Windowsize):
    TS = {} # Topics Status
    i = 0 # row count
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        first_row = next(csv_reader)
        for index in range(len(first_row)):
            TS[index] = Windowsize # init each topic to Windowsize, indicating there is no windowsize constrain at this point
        for row in csv_reader:  # read a line in source csv
            l = []
            for idx, alert in enumerate(row):   # work on each element in a line
                if alert == "1": # congested, split!
                    l = l+["SPLIT"]
                    TS[idx] = 0
                else:
                    TS[idx] = TS[idx]+1
                    if TS[idx] < Windowsize: # free "topic buffer" but the last operator is split and haven't reach to window size bound
                        l = l+["SKIP"]
                    else:   
                        l = l+["MERGED"]
                    
            yield l
            
            i = i + 1
    
    #        perodically delay, commented out for test purpose
            time.sleep(ProcessTimeDelay)
