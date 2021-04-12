import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import os
import time


FILENAMES = [x for x in os.listdir('logs') if 'flood' in x]
#print(FILENAMES)



def build_data(filenames):
    filedata = {}
    for file in filenames:
        #dfcolumns = pd.read_csv(basefile.format(file=file), nrows=1, delim_whitespace=True)
        df = pd.read_csv('logs/' + file, header=None, skiprows=15, engine = 'python', skipfooter = 10)
        filedata[file] = df
    return filedata

def parse_ip(seconds_log):
    pieces = seconds_log.split('.')
    # seconds10, 0, 0, x, log --> we want x
    #print(pieces)
    host = pieces[3]
    return int(host)

def main():
    data = build_data(FILENAMES)

    dataSet = sorted(data.keys(), key=lambda x: parse_ip(x))


    #fig = plt.figure()
    #ax = fig.add_subplot(111, projection="3d")

    #plt.plot(xaxis, yaxis) where xaxis and yaxis are lists
    plt.plot(dataSet, [data[f].median() for f in dataSet])
    plt.xticks(rotation=30)
    timestamp = str(round(time.time()))[-5:]

    #plt.show()
    plt.savefig('graphs/average_{}.png'.format(timestamp))

if __name__ == '__main__':
    main()


