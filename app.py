
import tqdm
import timeit
from os import path
import pandas as pd
chunksize = 10 ** 7


def getSpecifiedDataFromChunk(data):
    case = {
        "date": data['TextTime'].split(' ')[0],
        "province": data['NE Name'][:3]
    }
    return f'LTE KPI Backup EAS {case.get("province")}&{case.get("date")}.csv'


def getNameForCreateFiles(province, date):
    return f'LTE KPI Backup EAS {province}&{date}.csv'


def main():

    # Edit here
    pathForSearch = ' Edit your path here'
    countWriter = 0
    start = timeit.default_timer()
    result = []
    dateWithProvinceList = []
    print("Reading data ")

    for chunk in pd.read_csv(pathForSearch, chunksize=chunksize):
        result.append(chunk)

    print(type(result[0]))

    # get date from ['TextTime]
    for i in range(len(result[0])):
        chunkDate = result[0].iloc[i]['TextTime'].split(' ')[0]
        chunkProvince = result[0].iloc[i]['NE Name'][:3]
        if [chunkDate, chunkProvince] not in dateWithProvinceList:
            dateWithProvinceList.append([chunkDate, chunkProvince])

    # create file named date and generate the header
    print("Create file")
    for i in range(len(dateWithProvinceList)):
        dateFromList = dateWithProvinceList[i][0]
        provinceFromList = dateWithProvinceList[i][1]
        fileName = getNameForCreateFiles(
            province=provinceFromList, date=dateFromList)
        pathForChecking = f'./result/{fileName}'
        if path.isfile(pathForChecking):
            pass

        else:
            with open(pathForChecking, "x") as f:
                lengthResult = len(result[0].columns)
                for i in range(lengthResult):
                    f.write(f'{result[0].columns[i]}')
                    if i == len(result[0].columns) - 1:
                        f.write('\n')
                        continue
                    f.write(',')

    countWrongPath = 0
    for i in range(len(result[0])):
        dateWithProvinceOfChunk = getSpecifiedDataFromChunk(
            data=result[0].iloc[i])
        if path.isfile(f'./result/{dateWithProvinceOfChunk}'):
            result[0].iloc[[i]].to_csv(
                f'./result/{dateWithProvinceOfChunk}', index=False, header=False, mode='a')
            countWriter += 1


    print("files created")
    print('Len files : ', len(result[0]))
    print('Count writer', countWriter)
    stop = timeit.default_timer()
    print("Run time :", int(stop-start) // 60,
          " min ", int((stop-start) % 60), ' sec ')
    print('End')
    return


if __name__ == '__main__':
    main()
