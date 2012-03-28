from islandoraUtils import fileConverter as converter
from utils.commonFedora import *
import subprocess

""" ====== INGEST A SINGLE OBJECT ====== """
def createObjectFromFiles(fedora, config, objectData, extraNamespaces={}, extraRelationships={}):
    """
    Create a fedora object containing all the data in objectData and more
    """

    for ds in [ "TIFF" ]:
        # some error checking
        if not objectData['datastreams'].has_key(ds):
            # broken object
            print("Object data is missing required datastream: %s" % ds)
            return False

    #objPid = fedora.getNextPID(config.fedoraNS)
    objPid = "%s:%s" % (config.fedoraNS, objectData['label'])

    if config.dryrun:
        return True

    #extraNamespaces = { 'pageNS' : 'info:islandora/islandora-system:def/pageinfo#' }
    #extraRelationships = { fedora_relationships.rels_predicate('pageNS', 'isPageNumber') : str(idx+1) }

    # create the object (page)
    try:
        obj = addObjectToFedora(fedora, unicode("%s" % objectData['label']), objPid, objectData['parentPid'], objectData['contentModel'], extraNamespaces=extraNamespaces, extraRelationships=extraRelationships)
    except FedoraConnectionException, fcx:
        print("Connection error while trying to add fedora object (%s) - the connection to fedora may be broken", objPid)
        return False

    # === DYNAMIC DATASTREAM SECTION ===
    # these DS were defined by the Navigator as ID+source

    # ingest the datastreams we were given
    for dsid, file in objectData['datastreams'].iteritems():
        # hard coded blarg:
        if dsid in [ "MODS", "KML" ]:
            controlGroup = "X"
        else:
            controlGroup = "M"
        fedoraLib.update_datastream(obj, dsid, file, label=unicode(os.path.basename(file)), mimeType=misc.getMimeType(os.path.splitext(file)[1]), controlGroup=controlGroup)

    # === STATIC DATASTREAM SECTION ===
    # these DS are defined here - sources are created as required

    # ingest my custom datastreams for this object
    # create a JP2 datastream
    tifFile = objectData['datastreams']['TIFF']
    baseName = os.path.splitext(os.path.basename(tifFile))[0]

    jp2File = os.path.join(config.tempDir, "%s.jp2" % baseName)
    converter.tif_to_jp2(tifFile, jp2File, 'default', 'default') # this will generate jp2File
    fedoraLib.update_datastream(obj, u"JP2", jp2File, label=os.path.basename(jp2File), mimeType=misc.getMimeType("jp2"))
    os.remove(jp2File) # finished with that

    # i'm generating my own thumbnails
    tnFile = os.path.join(config.tempDir, "tmp.jpg")
    converter.tif_to_jpg(tifFile, tnFile, imageMagicOpts='TN')
    #add a TN datastream to the map object
    fedoraLib.update_datastream(obj, u"TN", tnFile, label=unicode(config.myCollectionName+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))
    os.remove(tnFile) # delete it so we can recreate it again for the next thumbnail
    # now tnFile is closed and deleted

    if config.jhoveCmd != None: # config.jhoveCmd will be empty if jhove extraction cannot be completed
        # extract mix metadata
        #cmd= jhove -h xml $INFILE | xsltproc jhove2mix.xslt - > `basename ${$INFILE%.*}.mix`
        mixFile = os.path.join(config.tempDir, "%s.mix.xml" % baseName)
        """ extract this into tif_to_mix() """
        outfile = open(mixFile, "w")
        jhoveCmd1 = ["jhove", "-h", "xml", tifFile]
        jhoveCmd2 = config.jhoveCmd
        p1 = subprocess.Popen(jhoveCmd1, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(jhoveCmd2, stdin=p1.stdout, stdout=outfile)
        r = p2.communicate()
        if os.path.getsize(mixFile) == 0:
            # failed for some reason
            print("jhove conversion failed")
        outfile.close()
        """ end extract """
        fedoraLib.update_datastream(obj, u"MIX", mixFile, label=os.path.basename(mixFile), mimeType=misc.getMimeType("xml"))
        os.remove(mixFile)

    return True
