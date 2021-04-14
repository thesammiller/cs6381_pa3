import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import os
import time



FILENAMES = [x for x in os.listdir('logs') if 'seconds' in x]


def build_data(filenames):
    filedata = {}
    for file in filenames:
        df = pd.read_csv('logs/' + file, header=None, skiprows=0, engine = 'python')
        filedata[file] = df
    return filedata

def parse_ip(seconds_log):
    pieces = seconds_log.split('.')
    # seconds10, 0, 0, x, log --> we want x
    # print(pieces)
    host = pieces[3]
    return int(host)

def main():
    data = build_data(FILENAMES)
    fig = plt.figure()
    #ax = fig.add_subplot(111, projection="2d")
    dataSet = sorted(data.keys(), key=lambda x: parse_ip(x))

    for enum, file in enumerate(data):
        plt.plot(dataSet, [data[f].quantile(0.90) for f in dataSet])
        plt.plot(dataSet, [data[f].quantile(0.95) for f in dataSet])
        plt.plot(dataSet, [data[f].quantile(0.99) for f in dataSet])
        '''
        zaxis = data[file].quantile(0.90)
        ax.plot(range(len(data)), zaxis)
    
        zaxis = data[file].quantile(0.95)
        ax.plot(range(len(data)), zaxis)
        
        zaxis = data[file].quantile(0.99)
        ax.plot(range(len(data)), zaxis)

        
        plt.plot(dataSet, [data[f].quantile(0.90) for f in dataSet])
        '''

    #dataSet = sorted(data.keys(), key=lambda x: parse_ip(x))
    plt.xticks(rotation=30)


    timestamp = str(round(time.time()))[-5:]

    fig.savefig('graphs/quantile_{}.png'.format(timestamp))


if __name__ == '__main__':
    main()


