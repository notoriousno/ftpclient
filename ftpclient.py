__author__ = 'nsarker@arinc.com'
__version__ = '0.02'
__status__ = 'alpha'


from twisted.python.failure import Failure
from twisted.protocols.ftp import FTPClient as _FTPClient
from twisted.internet.protocol import ClientCreator, Protocol
from os import getcwd, path as _path, makedirs
split = _path.split
exists = _path.exists
getsize = _path.getsize
join = _path.join

from twisted.internet.defer import Deferred, DeferredList
from twisted.internet import reactor


#---------- Classes ----------#
class FTPClient(object, _FTPClient):
    """ @not_implemented_yet
    """

    def __init__(self, user='anonymous', pswd='', pasv=True):
        """"""
        _FTPClient.__init__(self, username=user, password=pswd, passive=pasv)

    def GET(self, *f, **kwargs):
        """"""
        getList = []
        for files in f:
            d = Deferred()
            d.addCallback(self._checkFileTuple, files).addErrback(self.genericFail, 'Failed tuple check')
            d.addCallback(self._checkLocalPath).addErrback(self.genericFail, 'Wrong local path') # check local path exists
            d.addCallback(self._getFromFTP).addErrback(self.genericFail, 'Unable to get')
            getList.append(d)      # add to getList
            d.callback(None)
        return DeferredList(getList, consumeErrors=True)

    def _checkFileTuple(self, null, files):
        """
        """
        # files = self.getList.pop()
        ftpUrl = None
        localPath = None
        size = 0
        fromByte = 0
        
        try:
            ftpUrl, localPath, size, frombyte = files
        except:
            try:
                ftpUrl, localPath, size = files
            except:
                try:
                    ftpUrl, localPath = files
                except:
                    raise Exception('X--- Invalid file tuple.')
        return (ftpUrl, localPath, size, fromByte)

    def _checkLocalPath(self, ftpTuple):
        """ Checks whether or not the local path exists. If not, this fn will attempt to create it.
        """
        ftpUrl, localPath, size, fromByte = ftpTuple
        pth, fl = split(localPath)
        if not exists(pth):
            try:
                makedirs(pth)       # create dir if ! exist
                return ftpTuple
            except OSError as e:
                return Failure(e)   # do errback if unable to create dir
            except Exception as e:
                return Failure(e)   # do errback if unable to create dir
        else:
            if exists(localPath):
                fromByte = getsize(localPath)       # file exists so get the remaining bytes from FTP
            return (ftpUrl, localPath, size, fromByte)

    def _getFromFTP(self, ftpTuple):
        """
        """
        ftpUrl, localPath, size, fromByte = ftpTuple
        d = self.retrieveFile(path = ftpUrl,
            protocol = FileBuffer(
                    localPath, size),
            offset = fromByte)
        d.addCallbacks(self.successfulGET, callbackArgs=[ftpTuple],
            errback=self.failedGET, errbackArgs=[ftpTuple])
        return d


    #---------- Success Callbacks ----------#
    def successfulGET(self, result, ftpTuple):
        """"""
        self.quit()
        return ftpTuple


    #---------- Failure Callbacks ----------#
    def genericFail(self, result, msg):
        self.quit()
        return Failure(Exception('X--- {0}'.format(msg)))

    def failedGET(self, result, ftpTuple):
        """ """
        self.quit()
        return Failure(ftpTuple)


class FileBuffer(Protocol):
    """
    """
    def __init__(self, lclPath='', size=0):
        """
        :param name: Name of the file. Used to store on local drive.
        :param path: Local path that will store the file.
        """
        self.path, self.fileName = split(lclPath)
        if self.path == '':
            self.path = join(getcwd())      # set default path to the current directory
        self.localPath = join(lclPath)
        # try:
            # assert(_path.isdir(self.path))      # check if dir exists
            # if not exists(self.localPath):
                # with open(self.localPath, 'wb') as _file:
                    # _file.write(b'')        # create file if doesn't exist
        # except Exception as e:
            # raise Exception('X--- File path doesnt exist')      # dir or file doesn't exist

    def connectionMade(self):
        self.transport.registerProducer(self.transport, streaming=True)     # Registers the Producer (this case is the FTP data conn)

    def dataReceived(self, data):
        try:
            self.transport.pauseProducing()
            with open(self.localPath, 'a+b') as _file:
                _file.write(data)
            self.transport.resumeProducing()
        except Exception as e:
            self.transport.resumeProducing()
            Failure(e)
        finally:
            self.transport.resumeProducing()



#---------- Store Function ----------#
def __sendFile(consumer, bitLimit, localPath):
    """ Actually send a file.

    :param consumer:
    :param bitLimit:
    :param localPath:
    :return: FTPConnection
    """
    try:
        assert(exists(localPath))
    except Exception as e:
        Failure(e)

    try:
        with open(localPath, 'rb') as f:
            for chunk in iter(lambda: f.read(bitLimit), b''):       # chunk up the data
                consumer.write(chunk)           # ... and send
    except Exception as e:
        consumer.finish()
        Failure(e)
    finally:
        return consumer.finish()

def __putFile(ftpConnection, ftpPath, localFile, bitLimit=1024, offset=0):
    """ This callback fn will create the iConsumer obj that will be used to actually send the file.

    :param ftpConnection:
    :param url:
    :param offset:
    :return: FTPConnection
    """
    consumer, _list = ftpConnection.storeFile(ftpPath, offset)    # storeFile returns a Consumer and list of control-connection responses
    consumer.addCallback(__sendFile, bitLimit, localFile).addErrback(__fail, ftpConnection)
    return ftpConnection

def put(localPath, ftpPath, user='anonymous', pswd='', passive=True, bitLimit=1024, offset=0, host='127.0.0.1', port=21):
    """ Put the local file onto the FTP server.

    :param localPath:
    :param ftpPath:
    :param USER:
    :param PSWD:
    :param passive:
    :param bitLimit:
    :param offset:
    :return:
    """
    if not exists(localPath):
        return Failure(Exception("X--- File '{0}' doesn't exist".format(localPath)))
    ftpClient = ClientCreator(reactor, _FTPClient, USER, PSWD, passive=passive)
    ftpConnection = ftpClient.connectTCP(host, port)
    return ftpConnection.addCallback(__putFile, ftpPath, localPath, bitLimit, offset)


if __name__=='__main__':
    HOST = 'localhost'
    PORT = 21
    USER = 'noman'
    PSWD = 'qwerty'
    pasv = True

    #----- New Class based -----#
    def getFiles(client, *files, **kwargs):
        """
        :param files: Tuples (ftp_url, local_path, size_of_file, byte_offset) of which size of file and offset are **optional**.
        :param callback: A callback chain that will be executed after files are retrieved. (type: Deferred)
        """
        after = kwargs.get('after', Deferred())
        d = client.GET(*files, **kwargs).chainDeferred(after)
        print('')

    def afterGET(results):
        """"""
        for outcome in results:
            res, out = outcome
            if not res:
                print out.value
            else:
                print out

    ftpcli = ClientCreator(reactor, FTPClient, USER, PSWD, pasv)
    ftpconn = ftpcli.connectTCP(HOST, PORT)
    d = Deferred()
    d.addCallback(afterGET)
    ftpconn.addCallback(getFiles,
        ('large.zip', 'blh/LARGE.zip'),
        # ('scream.png', 'blah/scream.png'),
        # ('1', '2', 4, 5, 5, 6),
        ('ubntu.iso', 'blah/ubuntu.iso'),
        after = d)

    reactor.run()
