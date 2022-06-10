from datetime import date
import timeit
from os import path
import pandas as pd
chunksize = 10 ** 8


def getSpecifiedDataFromChunk(data):
    case = {
        "date" : data['TextTime'].split(' ')[0],
        "province" : data['NE Name'][:3]
    }
    return f'{case.get("province")}-{case.get("date")}.csv'

def main():
    start = timeit.default_timer()
    result = []
    dateWithProvinceList = []

    for chunk in pd.read_csv('./data/ *** Specify your file ***', chunksize=chunksize):
        result.append(chunk)

    # get date from ['TextTime]
    for i in range(len(result[0])):
        chunkDate = result[0].iloc[i]['TextTime'].split(' ')[0]
        chunkProvince = result[0].iloc[i]['NE Name'][:3]
        if [chunkDate,chunkProvince] not in dateWithProvinceList:
            dateWithProvinceList.append([chunkDate, chunkProvince])

    # create file named date and generate the header
    for i in range(len(dateWithProvinceList)):
        fileName = f'{dateWithProvinceList[i][1]}-{dateWithProvinceList[i][0]}.csv'
        if path.isfile(f'./result/{fileName}'):
            pass

        else:
            f = open(f'./result/{fileName}', "x")
            lengthResult = len(result[0].columns)
            for i in range(lengthResult):
                f.write(f'{result[0].columns[i]}')
                if i == len(result[0].columns) - 1:
                    continue
                f.write(',')

    countWriter = 0
    # # read write file
    for i in range(len(result[0])):
        dateWithProvinceOfChunk = getSpecifiedDataFromChunk(data=result[0].iloc[i])
        if path.isfile(f'./result/{dateWithProvinceOfChunk}'):
            result[0].iloc[[i]].to_csv(f'./result/{dateWithProvinceOfChunk}', index=False, header=False, mode='a')
            countWriter += 1

    print('Len files : ', len(result[0]))
    print('CountWriterFile : ', countWriter)
    stop = timeit.default_timer()
    print("Run time :", int(stop-start) // 60 ," min ", int((stop-start) % 60)  , ' sec ')
    print('End')
    return


if __name__ == '__main__':
    main()






