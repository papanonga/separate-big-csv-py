
from tqdm import tqdm, trange
import timeit
import os
from os import path
import pandas as pd
chunksize = 1000000
start = timeit.default_timer()


def getNameForCreateFiles(province, date):
    return f'LTE KPI Backup EAS {province}&{date}.csv'


def main():
    # Edit here
    pathForSearch = './data/LTE KPI Backup EAS 20220609.csv'
    countWriterTrue = 0
    countWriterFalse = 0
    dateWithProvinceList = []
    print("Processing data ")
    for _, chunk in enumerate(pd.read_csv(pathForSearch, chunksize=chunksize)):
        with tqdm(total=len(chunk)) as progress:
            for j in range(len(chunk)):
                # get time for create file
                chunkDate = chunk.iloc[j]['TextTime'].split(' ')[0]
                chunkProvince = chunk.iloc[j]['NE Name'][:3]
                fileName = getNameForCreateFiles(
                    province=chunkProvince, date=chunkDate)
                pathForChecking = f'./result/{chunkProvince}/{fileName}'
                if [chunkDate, chunkProvince] not in dateWithProvinceList:
                    dateWithProvinceList.append([chunkDate, chunkProvince])
                    if not path.exists(f'./result/{chunkProvince}'):
                        os.makedirs(f'./result/{chunkProvince}')
                    else:
                        pass

                    # create file if no file in directory
                    if path.isfile(pathForChecking):
                        pass
                    else:
                        with open(pathForChecking, "x") as f:
                            chunkColumnsLength = len(chunk.columns)
                            column = chunk.columns
                            for i in range(chunkColumnsLength):
                                f.write(f'{column[i]}')
                                if i == chunkColumnsLength - 1:
                                    f.write('\n')
                                    continue
                                f.write(',')


                # write data to their file
                if path.isfile(pathForChecking):
                    chunk.iloc[[j]].to_csv(
                        pathForChecking, index=False, header=False, mode='a')
                    countWriterTrue += 1
                else:
                    countWriterFalse += 1
                progress.update(1)

    stop = timeit.default_timer()
    print("Run time :", int(stop-start) // 60,
          " min ", int((stop-start) % 60), ' sec ')
    print("Count write true : ", countWriterTrue)
    print("Count write false : ", countWriterFalse)
    print('End')
    return


if __name__ == '__main__':
    main()
