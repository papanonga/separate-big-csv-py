
import timeit
from os import path
import pandas as pd
chunksize = 10 ** 8


def getSpecifiedDataFromChunk(data):
    case = {
        "date": data['TextTime'].split(' ')[0],
        "province": data['NE Name'][:3]
    }
    # return f'{}-{}.csv'
    return f'LTE KPI Backup EAS {case.get("province")}&{case.get("date")}.csv'


def main():

    # Edit here
    pathForSearch = 'Write your path here'

    fileNameList = []
    start = timeit.default_timer()
    result = []
    dateWithProvinceList = []

    for chunk in pd.read_csv(pathForSearch, chunksize=chunksize):
        result.append(chunk)

    # get date from ['TextTime]
    for i in range(len(result[0])):
        chunkDate = result[0].iloc[i]['TextTime'].split(' ')[0]
        chunkProvince = result[0].iloc[i]['NE Name'][:3]
        if [chunkDate, chunkProvince] not in dateWithProvinceList:
            dateWithProvinceList.append([chunkDate, chunkProvince])

    print("Chunk date,province = ", dateWithProvinceList)
    # create file named date and generate the header
    print("Create file")
    print(getSpecifiedDataFromChunk(data=result[0].iloc[0]))

    for i in range(len(dateWithProvinceList)):
        fileName = getSpecifiedDataFromChunk(data=result[0].iloc[i])
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
                    


    countWriter = 0

    # # read write file

    oldPath = getSpecifiedDataFromChunk(data=result[0].iloc[0])
    countWrongPath = 0

    for i in range(len(result[0])):
        dateWithProvinceOfChunk = getSpecifiedDataFromChunk(
            data=result[0].iloc[i])
        if oldPath != dateWithProvinceOfChunk:
            countWrongPath += 1
            oldPath = dateWithProvinceOfChunk

        if path.isfile(f'./result/{dateWithProvinceOfChunk}'):
            result[0].iloc[[i]].to_csv(
                f'./result/{dateWithProvinceOfChunk}', index=False, header=False, mode='a')
            countWriter += 1

    print('Len files : ', len(result[0]))
    stop = timeit.default_timer()
    print("Run time :", int(stop-start) // 60,
          " min ", int((stop-start) % 60), ' sec ')
    print("Wrong path : ", countWrongPath)
    print('End')

    return


if __name__ == '__main__':
    main()
