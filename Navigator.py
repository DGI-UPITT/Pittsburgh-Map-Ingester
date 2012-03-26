# -*- coding: utf8 -*-
from utils.commonFedora import *
import glob, zipfile, sys
import FileIngester

def get_immediate_subdirectories(dir):
    return [name for name in os.listdir(dir)
        if os.path.isdir(os.path.join(dir, name))]

""" ====== SCAN FOR OBJECTS IN A FOLDER ====== """
def processFolder(fedora, config):
    """
    Create a bunch of fedora objects (1 for each folder in @config.inDir)
    """

    folder = config.inDir

    # first make sure @folder is a valid folder
    if not os.path.isdir(folder):
        return False

    # the collection overhead
    # the host collection (topmost root)
    hostCollection = addCollectionToFedora(fedora, config.hostCollectionName, myPid=config.hostCollectionPid, tnUrl=config.hostCollectionIcon)
    # the aggregate (contains the books)
    myCollection = addCollectionToFedora(fedora, config.myCollectionName, myPid=config.myCollectionPid, parentPid=config.hostCollectionPid, tnUrl=config.myCollectionIcon)

    # this is the list of all folders to search in for books
    baseFileDict = { 'parentPid' : config.myCollectionPid, 'contentModel' : 'islandora:mapCModel' }
    totalFiles = 0
    completeFiles = 0
    for subFolder in os.listdir(folder):
        if os.path.isdir(os.path.join(folder, subFolder)):

            print("Scan Folder %s" % subFolder)

            fileDict = { 'label': subFolder, 'datastreams' : { } }

            def addFileByPattern(label, pattern):
                file = glob.glob("%s" % os.path.join(folder, subFolder, pattern))
                if len(file) > 0:
                    fileDict['datastreams'][label] = file[0]
                    return True
                return False

            if not addFileByPattern("TIFF", "*.tif"):
                if not addFileByPattern("TIFF", "*.tiff"):
                    # failed
                    print("Could not find base tif file - skipping directory %s" % subFolder)
                    continue # next subFolder
            addFileByPattern("MODS", "*.mods.xml")
            addFileByPattern("DC", "*.dc.xml")
            addFileByPattern("FGDC", "*.fgdc.xml")

            # creation of the dictionary here might be bad
            fileDict.update(baseFileDict)
            totalFiles = totalFiles + 1
            if FileIngester.createObjectFromFiles(fedora, config, fileDict):
                print("Object (%s) ingested successfully" % subFolder)
                completeFiles = completeFiles + 1

                # now ingest the georef clips
                for georefclip in get_immediate_subdirectories(os.path.join(folder, subFolder)):
                    # clear the datastreams list for the georef clips
                    subFileDict = { 'label': georefclip, 'datastreams' : {} }
                    # georef clip metadata
                    # maybe perform a subloop on these after object creation?
                    if not addFileByPattern("TIFF", "%s/*.tif" % georefclip):
                        if not addFileByPattern("TIFF", "%s/*.tiff" % georefclip):
                            # failed
                            print("Could not find base tif file - skipping georefclip directory %s" % georefclip)
                            continue # next subFolder
                    addFileByPattern("CNTRLP", "%s/*.controlpts.txt" % georefclip)
                    addFileByPattern("CNTRLPXML", "%s/*.controlpts.txt.xml" % georefclip)
                    addFileByPattern("TFWX", "%s/*.tfwx" % georefclip)
                    addFileByPattern("AUX", "%s/*.aux" % georefclip)
                    addFileByPattern("RRD", "%s/*.rrd" % georefclip)
                    addFileByPattern("TIFXML", "%s/*.tif.xml" % georefclip)
                    FileIngester.createObjectFromFiles(fedora, config, fileDict)

    return completeFiles
