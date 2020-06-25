import secrets
import random
import string
import os, sys
import json
from time import sleep
import subprocess

# выдать 64 байтовую строку
def generateRandomString(stringLength=64):
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(stringLength))

# выдать 1 килобайт рандомного месива
def generateRandomBytes(n=1024):
    return bytearray(random.getrandbits(8) for i in range(n))

# разбить имя файла на папки и остаток имени
def separateString(string, folders='', deep=4, step=2):
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
            return False
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
        fileList.append(random.randint(1, filesQueueSize * (seed - 1)))
    return fileList

def saveStats():
    with open(configFile, 'w') as outfile:
        json.dump(stats, outfile)
    outfile.close()

def loadStats():
    global stats
    with open(configFile, 'r') as infile:
        data = json.load(infile)
    print('data:', data)
    stats.update(data)
    printStats()
    infile.close()

def initVars():
    try:
        poolName = sys.argv[1]
    except Exception as e:
        print('Pool name not chosen')
        exit(0)
    global stats
    global configFile
    configFile = os.path.join(os.getcwd() + os.path.sep + poolName + '.zft')
#    getMountPointCmd = 'zfs list -H -o mountpoint ' + poolName
#    p = subprocess.Popen(getMountPointCmd, shell=True, stdout=subprocess.PIPE)
#    stats['mountpoint'] = p.stdout.read()
    stats['cycleNumber'] = 1
    stats['filesWrittenCount'] = 0
    stats['filesDeletedCount'] = 0
    stats['bytesWrittenCount'] = 0
    stats['bytesDeletedCount'] = 0
    random.seed(0)
    global minFileSize
    minFileSize = 1 #kb
    global maxFileSize
    maxFileSize = 2049 #kb
    global createdRandomBytes
    createdRandomBytes = generateRandomBytes(maxFileSize * 1024)
    global dataFolderName
    dataFolderName = os.path.join(os.getcwd() + os.path.sep + 'data')
    global filesQueueSize
    filesQueueSize = 10
    global fileRemovePercent
    fileRemovePercent = 0.3

def firstRun():
    if not os.path.exists(configFile):
        return True
    else:
        return False

def printStats():
#    print(stats)
    print(
    'fWC:', stats['filesWrittenCount'],
    'fDC:', stats['filesDeletedCount'],
    'bWC:', stats['bytesWrittenCount'],
    'bDC:', stats['bytesDeletedCount']#, end="\r"
    )


stats = {}
initVars()
printStats()
if not firstRun():
    loadStats()
sleep(2)

while True:
    rangeBegin = (stats['cycleNumber'] - 1) * filesQueueSize + 1
    rangeEnd = stats['cycleNumber'] * filesQueueSize + 1
    for i in range(rangeBegin, rangeEnd):
        bytesWritten = touchFileFromSeed(i)
        if bytesWritten:
            stats['bytesWrittenCount'] += bytesWritten
            stats['filesWrittenCount'] += 1
            sleep(0.01)
#    print('seed for remove:', stats['cycleNumber'])
    removeList = generateRandomRemoveQueueList(stats['cycleNumber'])
    if removeList:
        for item in removeList:
            bytesDeleted = removeFileFromSeed(item)
            if bytesDeleted:
                stats['bytesDeletedCount'] += bytesDeleted
                stats['filesDeletedCount'] += 1
#                print(item, bytesDeleted, stats['bytesDeletedCount'])
    stats['cycleNumber'] += 1
    saveStats()
    printStats()
    sleep(1)
