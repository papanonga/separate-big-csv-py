import timeit
from os import path
import pandas as pd
chunksize = 1500 * 10 ** 8


def main():
    start = timeit.default_timer()
    dateHistory = []
    result = []

    for chunk in pd.read_csv('specify your scanning directory here', chunksize=chunksize):
        result.append(chunk)

    # get date from ['TextTime] 
    for i in range(len(result[0])):
        chunkDate = result[0].iloc[i]['TextTime'].split(' ')[0]
        if chunkDate not in dateHistory:
            dateHistory.append(chunkDate)

    # create file named date and generate the header
    for i in range(len(dateHistory)):
        if path.isfile(f'./result/{dateHistory[i]}.csv'):
            pass

        else:
            f = open(f'./result/{dateHistory[i]}.csv', "x")
            lengthResult = len(result[0].columns)
            for i in range(lengthResult):
                f.write(f'{result[0].columns[i]}')
                if i == len(result[0].columns) - 1:
                    continue
                f.write(',')

    countWriter = 0
    # read write file 
    for i in range(len(result[0])):
        dateFromChunk = result[0].iloc[i]['TextTime'].split(' ')[0]
        if path.isfile(f'./result/{dateFromChunk}.csv'):
            result[0].iloc[[i]].to_csv(f'./result/{dateFromChunk}.csv', index=False, header=False, mode='a')
            countWriter += 1

    print('Len files : ', len(result[0]))
    print('CountWriterFile : ', countWriter)
    stop = timeit.default_timer()
    print("Run time start : ", start ,"sec")
    print("Run time stop : ", stop, "sec")
    print("Run time :", stop-start , "sec")
    print('End')
    return


if __name__ == '__main__':
    main()
