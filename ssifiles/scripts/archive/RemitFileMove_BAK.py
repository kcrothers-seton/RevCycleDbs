import os, string, datetime, time, glob, zipfile
from shutil import copy
from shutil import move
import win32com.client

extractpath = '\\\\AUSSSI\\SSIAPP\\RENWS\\BILLING\\UPLOADH'
remitarchivepath = os.path.join(extractpath,'UB')
filetype = 'BAK'


def archive(remitarchivepath,remityear,filetype,remitfile,remitfilename):
    try:
        zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' + '.zip'),'r')
    except IOError, BadZipfile:
        zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'w')
        zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
        zfile.close()
    else:
        try:
            zfile.read(remitfile)
        except:
            zfile = zipfile.ZipFile(os.path.join(remitarchivepath, remityear + '_'+ filetype + 's' +'.zip'),'a')
            zfile.write(os.path.join(extractpath, remitfile),os.path.basename(remitfile), zipfile.ZIP_DEFLATED)
            zfile.close()

for remitfile in glob.glob(os.path.join(remitarchivepath, '*.' + filetype)):
    remitfilename = os.path.split(remitfile)[1]
    print remitfilename
    try:
        remitfiledate = datetime.datetime.strptime(remitfilename[3:9],'%m%d%y')
        remityear = remitfiledate.strftime('%Y')
        archive(remitarchivepath,remityear,filetype,remitfile,remitfilename)
        os.remove(remitfile)
    except:
        print 'archive failed: ' + remitfilename
