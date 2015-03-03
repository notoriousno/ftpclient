__author__ = 'nsarker@arinc.com'
__version__ = '0.01'
__status__ = 'alpha'


from twisted.python.failure import Failure
from twisted.protocols.ftp import FTPClient as _FTPClient
from twisted.internet.protocol import ClientCreator, Protocol
from os import getcwd, path as _path
split = _path.split
exists = _path.exists
getsize = _path.getsize
join = _path.join

from twisted.internet import reactor


#---------- Success ----------#
def __success(success, ftpConnection):
    print('[X] Successful Transaction')
    try:
        for x in success:
            print(x)
    except Exception as e:
        print(success)

    try:
        return ftpConnection.quit()
    except:
        print('\nX--- Unable to quit the FTP connection upon success.')
        return ftpConnection

def __successfulGET(success, ftpConnection, ftpFile, localFile):
    print('[X] Successful GET details:\nFTP File: {0}\nLocal File: {1}\n'.format(ftpFile, localFile))
    try:
        return ftpConnection.quit()
    except:
        print('\nX--- Unable to quit the FTP connection upon success.')
        return ftpConnection


#---------- Failures ----------#
def __fail(failure, ftpConnection):
    print('\nX--- Failed transaction')
    try:
        for e in failure:
            print(e)
    except Exception as e:
        print(failure)

    try:
        return ftpConnection.quit()
    except:
        print('\nX--- Unable to quit the FTP connection upon failure.')
        return ftpConnection


#---------- Classes ----------#
class FTPClient(object):
    """ @not_implemented_yet
    """


class FileBuffer(Protocol):
    """
    """
    def __init__(self, lclPath=''):
        """
        :param name: Name of the file. Used to store on local drive.
        :param path: Local path that will store the file.
        """
        self.path, self.fileName = split(lclPath)
        if self.path == '':
            self.path = join(getcwd())      # set default path to the current directory
        self.localPath = join(lclPath)
        try:
            assert(_path.isdir(self.path))      # check if dir exists
            if not exists(self.localPath):
                with open(self.localPath, 'wb') as _file:
                    _file.write(b'')        # create file if doesn't exist
        except Exception as e:
            raise Exception('X--- File path doesnt exist')      # dir or file doesn't exist

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


#---------- Get Function ----------#
def __getFile(ftpConnection, url, lclPath=None):
    """ This function will actually retrieve the file and transfer it to a local destination.

    @fix This fn is inefficient. Must find better way of not downloading something when file is complete

    :param ftpConnection:
    :param url:
    :param lclPath:
    :return: FTPConnection
    """
    offsetByte = 0

    if not lclPath:
        pth, fileName = split(url)
        lclPath = join(getcwd(), fileName)      # get cwd path

    if not exists(lclPath):
        # with open(filePath, 'wb') as _file:
        #     _file.write(b'')        # create file if doesn't exist
        offsetByte = 0      # start download from the beginning
    else:
        offsetByte = getsize(lclPath)

    localFile = None
    try:
        localFile = FileBuffer(lclPath=lclPath)
    except Exception as e:
        Failure(e)

    ftpConnection.retrieveFile(
        url,
        localFile,
        offset = offsetByte).addCallbacks(__successfulGET, callbackArgs=[ftpConnection, localFile, url],
                                          errback=__fail, errbackArgs=[ftpConnection])
    return ftpConnection

def get(url, localFile=None, size=None, offset=0, user='anonymous', pswd='', passive=True, host='127.0.0.1', port=21):
    """

    :param fileNPath:
    :param size:
    :param USER:
    :param PSWD:
    :param passive:
    :return: FTPConnection
    """
    ftpClient = ClientCreator(reactor, _FTPClient, user, pswd, passive=passive)
    ftpConnection = ftpClient.connectTCP(host, port)
    ftpConnection.addCallback(__getFile, url=url, lclPath=localFile).addErrback(__fail, ftpClient)  # @fix Use better Errback
    return ftpConnection


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
    if not exists(ftpPath):
        return Failure(Exception("X--- File '{0}' doesn't exist".format(ftpPath)))
    ftpClient = ClientCreator(reactor, _FTPClient, USER, PSWD, passive=passive)
    ftpConnection = ftpClient.connectTCP(host, port)
    return ftpConnection.addCallback(__putFile, ftpPath, localPath, bitLimit, offset)


if __name__=='__main__':
    HOST = 'localhost'
    PORT = 21
    USER = 'noman'
    PSWD = 'qwerty'

    #----- Basic get from FTP -----#
    # get('large.zip', localFile='LARGE.zip', user=USER, pswd=PSWD, host=HOST, port=PORT)

    #----- Get from file from a subfolder and put in a local subfolder ----#
    # get('test/large.zip', 'dir/LARGE.zip', user=USER, pswd=PSWD)


    #----- Basic put to FTP -----#
    put('large.zip', ftpPath='test/large.zip', user=USER, pswd=PSWD, host=HOST, port=PORT)
    reactor.run()
