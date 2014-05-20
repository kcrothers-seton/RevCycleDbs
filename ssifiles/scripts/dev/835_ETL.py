import os, glob, string, zipfile, datetime,ftplib
from shutil import move
from shutil import copy

xp = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\835\\Today'
lp = 'G:\\SHN-FTP-XFR\\IS-Apps\\RCPSI\\835'

filetype = '835'
f_ptrn = '*.' + filetype

logpath = 'N:\\PFS-DSS\\RevCycleDBs\ssifiles\\logs\\835'

class File():
    def __init__(self,filepath,filename):
        self.filepath = filepath
        self.filename = filename
        self.file = os.path.join(filepath,filename)
        self.filetype = string.split(self.filename,'.',)[-1]        
        self.filenamenoext = string.join(string.split(self.filename,'.',)[:-1],'')
        
    def archive(self,archivepath,zipfilename):
        zipfullfilename = os.path.join(archivepath,zipfilename)
        try:
            zfile = zipfile.ZipFile(zipfullfilename, 'r')
        except IOError, BadZipfile:
            try:
                zfile = zipfile.ZipFile(zipfullfilename, 'w')
                zfile.write(self.file, self.filename, zipfile.ZIP_DEFLATED)
                zfile.close()
                os.remove(self.file)
            except IOError, BadZipfile:
                pass
        else:
            try:
                zfile = zipfile.ZipFile(zipfullfilename, 'a')
                zfile.write(self.file, self.filename, zipfile.ZIP_DEFLATED)
                zfile.close()
                os.remove(self.file)
            except IOError, BadZipfile:
                pass


class Txt(File):
    def filetext(self):
        self.filereader = open(self.file,'r')
        self.filetext = self.filereader.read()
        self.filereader.close()
    def filelines(self):
        self.filereader = open(self.file,'r')
        self.filelines = self.filereader.readlines()
        self.filereader.close()
    def write(self,text):
        if os.path.isfile(self.file):
            hndlr = open(self.file,'a')
        else:
            hndlr = open(self.file,'w')            
        hndlr.write(text)
        hndlr.close()
    def writelines(self,text):
        if os.path.isfile(self.file):
            hndlr = open(self.file,'a')
        else:
            hndlr = open(self.file,'w')            
        hndlr.writelines(text)
        hndlr.close()



def ETL():
    xfiles = glob.glob(os.path.join(xp,f_ptrn))
    zipfilename = today.strftime('%y%m%d') + '_' + filetype + 's.zip'
    for f in xfiles:
        fn = os.path.basename(f)
        f_obj = File(xp,fn)
        f_obj.archive(lp,zipfilename)
    load_file = os.path.join(lp,zipfilename)
    return load_file


def start():
    global today
    today = datetime.datetime.today()
    log_fn = 'log_' + filetype + '_' + today.strftime('%Y-%m-%d_%H%M') + '.txt'
    log = Txt(logpath,log_fn)
    log.write('job started at ' + datetime.datetime.now().ctime() + '\n\n')
    return log

log = start()
load_file = ETL()

log.write('job ended at ' + datetime.datetime.now().ctime() + '\n\n')

  