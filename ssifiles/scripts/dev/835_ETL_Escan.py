import os, glob, string, zipfile, datetime
from shutil import move
from shutil import copy
from file_hndlrs import File3,Txt3
import inspect,traceback


logpath = 'N:\\PFS-DSS\\RevCycleDBs\ssifiles\\logs\\835'

xp = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\835\\Today'
lps = {'*.835':'G:\\SHN-FTP-XFR\\IS-Apps\\RCPSI\\835',
       '*car*.835':logpath}
       #'car*.835':'\\\\Ausiis01\\shn-ftp-xfr\\IS-Apps\\E-Scan'}

filetype = '835'

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


def start():
    global today
    today = datetime.datetime.today()
    log_fn = 'log_' + filetype + '_' + today.strftime('%Y-%m-%d_%H%M') + '.txt'
    global log
    log = Txt3(None,logpath,log_fn)
    log.write('job started at ' + datetime.datetime.now().ctime() + '\n\n')
    return log

def ETL():
    try:
        xfs = []
        for fn_ptrn,lp in lps.items():
            files_zipped = 0
            xfiles = glob.glob(os.path.join(xp,fn_ptrn))
            numfound = log_foundfiles(xfiles)
            zipfilename = today.strftime('%y%m%d') + '_' + filetype + 's.zip'
            transform_file = os.path.join(xp,zipfilename)
            load_file = os.path.join(lp,zipfilename)
            for xf in xfiles:
                fn = os.path.basename(xf)
                f = File3(None,xp,fn)
                f.archive(xp,zipfilename,remove=False)
                files_zipped += 1
            log.write(str(files_zipped) + ' files written to zip file: ' + zipfilename + '.\n\n')
            checkstatus = validate(numfound,files_zipped)
            if checkstatus and files_zipped > 0:
                move(transform_file,load_file)
                xfs += xfiles
        for xf in list(set(xfs)):
            os.remove(xf)
    except:
        log.write('Function ETL failure.\n')
        log.write(traceback.print_exc())
        

def log_foundfiles(xfiles):
    numfound = len(xfiles)
    log.write(str(numfound) + ' files found in ' + xp + ':\n\n')
    for xf in xfiles:
        log.write(os.path.basename(xf) + '\n')
    log.write('\n')
    return numfound

def validate(numfound,files_zipped):
    checkstatus = numfound == files_zipped
    if not checkstatus:
        log.write('ERROR: ETL validation failure.')
        log.write('Files found: ' + str(numfound) + '.')
        log.write('Files zipped: ' + str(files_zipped) + '.')
        log.write('File count discrepancy: ' + str(numfound - files_zipped) + '.')
        osCommandString = 'notepad.exe ' + log.file
        os.system(osCommandString)
    return checkstatus

def set_test_paths(xp,lps):
    xp = os.path.join(xp,'test')
    for k,lp in lps.items():
        lps[k] = os.path.join(lp,'test')
    return xp,lps
    
xp,lps = set_test_paths(xp,lps)
log = start()
load_file = ETL()
log.write('job ended at ' + datetime.datetime.now().ctime() + '\n\n')
