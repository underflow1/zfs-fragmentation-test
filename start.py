import secrets
import random
import string
import os, sys
import json
import time
import subprocess
import shutil
import math

# выдать 64 байтовую строку
def generateRandomString(stringLength=64):
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(stringLength))

# выдать 1 килобайт рандомного месива
def generateRandomBytes(n=1024):
    return bytearray(random.getrandbits(8) for i in range(n))

# разбить имя файла на папки и остаток имени
def separateString(string, folders='', deep=2, step=1):
    if len(string) < deep * step: return False
    if deep == 0: return {'folders': folders, 'filename': string}
    else:
        folders += string[0:step] + os.path.sep
        return separateString(string[step:], folders, deep - 1)

# создать и записать файл по номеру
def touchFileFromSeed(seed):
    random.seed(seed)
    currentFileMetadata = separateString(generateRandomString())
    if currentFileMetadata:
        currentFilePath = dataFolderName + os.path.sep + currentFileMetadata['folders']
        if not os.path.exists(currentFilePath):
            try:
                os.makedirs(currentFilePath)
            except OSError:
                print ("Creation of the directory %s failed" % currentFilePath)
#                return False
        try:
            currentFileSize = random.randint(minFileSize, maxFileSize) * 1024
            f = open(currentFilePath + currentFileMetadata['filename'], "wb")
            f.write(createdRandomBytes[0:currentFileSize])
            f.close()
        except OSError:
            print ("Creation of the file %s failed" % currentFileMetadata['filename'])
            exit(0)
    return currentFileSize

# удалить файл по номеру
def removeFileFromSeed(seed):
    random.seed(seed)
    currentFileMetadata = separateString(generateRandomString())
    currentFilePath = dataFolderName + os.path.sep + currentFileMetadata['folders']
    try:
        os.remove(currentFilePath + currentFileMetadata['filename'])
    except OSError as e:
        return False
    currentFileSize = random.randint(minFileSize, maxFileSize) * 1024
    return currentFileSize

# список файлов на удаление
def generateRandomRemoveQueueList(seed):
    random.seed(seed)
    if seed < 10:
        return False
    fileList = []
    filesToRemoveCount = int(filesQueueSize * fileRemovePercent)
    for i in range(0, filesToRemoveCount):
        fileList.append(random.randint(filesQueueSize * (seed - 3), filesQueueSize * (seed - 2)))
    return fileList

def saveStats():
    with open(configFile, 'w') as outfile:
        json.dump(stats, outfile)
    outfile.close()

def loadStats():
    global stats
    with open(configFile, 'r') as infile:
        data = json.load(infile)
    stats.update(data)
    infile.close()

def writeSummaryResults(data):
    output = open(progressResultsFilePath, 'a')
    output.write(data + '\n')
    output.close()

def writeProgressResults(capacity):
    fragCmd = 'zpool list ' + poolName + ' | tail -n 1 | awk \'{print $7}\''
    iopingCmd = 'ioping ' + dataFolderName + '/' + generateRandomString(1) + ' -D -c 5 | tail -n 1 | awk \'{print $6$7}\'\'\t\' | tr -d \'\n\' >> ' + progressResultsFilePath + '; cat tab >> ' + progressResultsFilePath
    writeSpeedCmd = 'zpool iostat ' + poolName + ' 15 2 | tail -n 1 | awk \'{print $7}\' >> ' + progressResultsFilePath
    try:
        frag = subprocess.Popen(fragCmd, shell=True, stdout=subprocess.PIPE)
        fragResult = frag.stdout.read().decode("utf-8").rstrip()
        output = open(progressResultsFilePath, 'a')
        output.write(str(capacity) + '\t' + fragResult + '\t')
        output.close()
        ioping = subprocess.Popen(iopingCmd, shell=True, stdout=subprocess.PIPE)
        writeSpeed = subprocess.Popen(writeSpeedCmd, shell=True, stdout=subprocess.PIPE)
        return True
    except Exception as e:
        print('something nasty happened: ' + str(e))
        exit(0)

def firstRun():
    if not os.path.exists(configFile):
        print('this is first run\n')

        output = open(progressResultsFilePath, 'a')
        firstRunCmd = 'zfs get -H recordsize,compression ' + poolName
        intro = subprocess.Popen(firstRunCmd, shell=True, stdout=subprocess.PIPE).stdout.read().decode("utf-8").rstrip()
        header = "CAP	FRAG	IOPING	WSPEED"
        output.write(intro + '\n')
        output.write(header + '\n')
        output.close()
        return True
    else:
        print('previous run was detected, continue filling up the pool\n')
        return False

def formatSize(size):
    "Formats size to be displayed in the most fitting unit"
    power = math.floor((len(str(abs(int(size))))-1)/3)
    units = {
            0: " B",
            1: "KB",
            2: "MB",
            3: "GB",
            4: "TB",
            5: "PB",
            6: "EB",
            7: "ZB",
            8: "YB"
        }
    unit = units.get(power)
    sizeForm = size / (1000.00**power)
    return "{:3.2f} {}".format(sizeForm, unit)

def freeSpaceExists():
    getFreeSpaceCmd = 'zfs list -Hp -o available ' + poolName
    p = subprocess.Popen(getFreeSpaceCmd, shell=True, stdout=subprocess.PIPE)
    freeSpace = int(p.stdout.read().decode("utf-8").rstrip())
    if freeSpace < minFreeSpace:
        return False
    else:
        return True

def printStats():
    print("\033[2;3H")
    print('Файлов создано:', stats['filesWrittenCount'], '  Удалено:', stats['filesDeletedCount'])
    print('Байтов записано:', formatSize(stats['bytesWrittenCount']), ' Удалено:', formatSize(stats['bytesDeletedCount']))
    print('ИТОГО ЗАПИСАНО СЕЙЧАС:', formatSize(stats['bytesWrittenCount'] - stats['bytesDeletedCount']))

clear = lambda: os.system('clear')
clear()
stats = {}

#initVars
if True:
    try:
        poolName = sys.argv[1]
    except Exception as e:
        print('Pool name not chosen')
        exit(0)
    try:
        recordSize = sys.argv[2]
    except Exception as e:
        print('recordsize not chosen')
        exit(0)
    try:
        compression = sys.argv[3]
    except Exception as e:
        print('compression not chosen')
        exit(0)
    configFile = os.path.join(os.getcwd() + os.path.sep + poolName + '.zft')
    stats['cycleNumber'] = 1
    stats['filesWrittenCount'] = 0
    stats['filesDeletedCount'] = 0
    stats['bytesWrittenCount'] = 0
    stats['bytesDeletedCount'] = 0
    random.seed(0)
    minFileSize = 1 #kb
    maxFileSize = 2049 #kb
    createdRandomBytes = generateRandomBytes(maxFileSize * 1024)
    getMountPointCmd = 'zfs list -H -o mountpoint ' + poolName
    p = subprocess.Popen(getMountPointCmd, shell=True, stdout=subprocess.PIPE)
    mountpoint = p.stdout.read().decode("utf-8").rstrip()
    dataFolderName = os.path.join(mountpoint + os.path.sep + 'zfs-fragmentation-test-data')
    filesQueueSize = 100
    fileRemovePercent = 0.3
    # заканчивать когда будет гиг свободного места
    minFreeSpace = 1073741824
    summaryResultsFilePath = os.getcwd() + os.path.sep + 'results.zfs'
    progressResultsFilePath = os.getcwd() + os.path.sep + 'progress_' + poolName + '_' + recordSize + '_' + compression + '_' + time.strftime("%Y%m%d%H%M%S") + '.zft'
    print(progressResultsFilePath)
    lastCapacity = 0
# endinit

def getCurrentCapacity():
    getCurrentCapacityCmd = 'zpool list ' + poolName + ' | tail -n 1 | awk \'{print $8}\''
    p = subprocess.Popen(getCurrentCapacityCmd, shell=True, stdout=subprocess.PIPE)
    capacity = p.stdout.read().decode("utf-8").rstrip().strip('%')
    return int(capacity)


if not firstRun():
    loadStats()

cycleStartTime = time.time()
while True:
    if not freeSpaceExists():
        print('Free space is ended. Finishing')
        break
    if lastCapacity < getCurrentCapacity() and getCurrentCapacity() % 2:
        writeProgressResults(getCurrentCapacity())
        lastCapacity = getCurrentCapacity()
    rangeBegin = (stats['cycleNumber'] - 1) * filesQueueSize + 1
    rangeEnd = stats['cycleNumber'] * filesQueueSize + 1

    for i in range(rangeBegin, rangeEnd):
        bytesWritten = touchFileFromSeed(i)
        if bytesWritten:
            stats['bytesWrittenCount'] += bytesWritten
            stats['filesWrittenCount'] += 1

    removeList = generateRandomRemoveQueueList(stats['cycleNumber'])
    if removeList:
        for item in removeList:
            bytesDeleted = removeFileFromSeed(item)
            if bytesDeleted:
                stats['bytesDeletedCount'] += bytesDeleted
                stats['filesDeletedCount'] += 1

    stats['cycleNumber'] += 1
    saveStats()
    printStats()
cycleFinishTime = time.time()
cycleElapsedTime = cycleFinishTime - cycleStartTime

writeSummaryResults('Длительность записи: ' + str(cycleElapsedTime))

cycleStartTime = time.time()
readAllCmd = 'find ' + dataFolderName + ' -type f -exec cat {} + > /dev/null'
#readAllCmd='sleep 5'
p = subprocess.Popen(readAllCmd, shell=True, stdout=subprocess.PIPE)
p.wait()
cycleFinishTime = time.time()
cycleElapsedTime = cycleFinishTime - cycleStartTime

writeSummaryResults('time is ' + time.strftime("%Y%m%d%H%M%S"))
writeSummaryResults('Длительность чтения: ' + str(cycleElapsedTime))
writeSummaryResults('Записано файлов: ' + str(stats['filesWrittenCount'] - stats['filesDeletedCount']) + ' Записано байтов: ' + str(stats['bytesWrittenCount'] - stats['bytesDeletedCount']) + '\n')

saveStats()
os.remove(configFile)
