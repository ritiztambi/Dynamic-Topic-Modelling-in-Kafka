import csv
import time
import sys


# python3 evoker.py "./test_dataset.csv"


# parameters
# ============================================
TarHist = sys.argv[1]   # target history file

ProcessTimeDelay = 2    # seconds
Windowsize = 5  # number of records skipped
                # assuming windowsize delay only apply for "0" signal after a split takes place.
                # i.e. after a split take place, algorithm can directly split again but can only merge after leaving window bound
                # merge won't cause window delay. i.e. after a merge takes place, algorithm can directly merge or split based on signal.
                # once left the window, can do either split or merge.
# ============================================


TS = {} # Topics Status
i = 0 # row count
with open(TarHist) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    first_row = next(csv_reader)
    for index in range(len(first_row)):
        TS[index] = Windowsize # init each topic to Windowsize, indicating there is no windowsize constrain at this point
    for row in csv_reader:  # read a line in source csv

        l = []
        for idx, alert in enumerate(row):   # work on each element in a line
            if alert == "1": # congested, split!
                # print("( "+str(idx)+", "+str(i)+" )"+" split!!!!")
                l = l+["splitted"]
                TS[idx] = 0
            elif alert == "0" and TS[idx] < Windowsize: # free "topic buffer" but the last operator is split and haven't reach to window size bound
                l = l+["skipped "]
                TS[idx] = TS[idx]+1
            else:   # alert == "0" and TS[idx] >= Windowsize; Free "topic buffer" and left the window bound
                # print("( "+str(idx)+", "+str(i)+" )"+" merge!!!!!!")
                l = l+["MERGED  "]
                TS[idx] = Windowsize
        print(l+[str(i)])
        
        i = i + 1

        # perodically delay, commented out for test purpose
        # time.sleep(ProcessTimeDelay)


        # the orginal signal value for compare
        # print(row+[str(i)])
