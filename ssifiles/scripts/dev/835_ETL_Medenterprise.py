import os, glob, string, zipfile, datetime
from shutil import move
from shutil import copy
from file_hndlrs import File3,Txt3
import inspect,traceback
from job_tools import set_test_paths

logpath = r'\\txaus.ds.sjhs.com\apps\PFS-DSS\RevCycleDBs\ssifiles\logs\835'

xp = r'\\ausssi\ssiapp\renws\billing\uploadh\835\Today'

dest_fnptrns = {'Vendors':['*.835'],
                'E-scan':['*car*.835'],
                'MedEnterprise':['*car*.835','*cad*.835']

dest_lps = {'Vendors':r'\\seton\groupdir\SHN-FTP-XFR\IS-Apps\RCPSI\835',
            'E-scan':r'\\seton\groupdir\shn-ftp-xfr\IS-Apps\E-Scan',
            'MedEnterprise':r'\\seton\groupdir\SHN-FTP-XFR\IS-Apps\RCPSI\MedEnterprise_835'}

filetype = '835'


def start():
    global today
    today = datetime.datetime.today()
    log_fn = 'log_' + filetype + '_' + today.strftime('%Y-%m-%d_%H%M') + '.txt'
    global log
    log = Txt3(None,logpath,log_fn)
    log.write('Job started at ' + datetime.datetime.now().ctime() + '\n\n')
    return log

def find_dest_files(dest,fn_ptrns):
    xfiles = []
    for fn_ptrn in fn_ptrns:
        xfiles += glob.glob(os.path.join(xp,fn_ptrn))
    return xfiles

def compress(xfiles):
    numfiles_zipped = 0
    zipfilename = today.strftime('%y%m%d') + '_835s.zip'
    for xf in xfiles:
        fn = os.path.basename(xf)
        f = File3(None,xp,fn)
        f.archive(xp,zipfilename,remove=False)
        numfiles_zipped += 1
    return numfiles_zipped 

def ETL():
    try:
        xfs = []
        for dest, fn_ptrns in dest_fnptrns.items():
            xfiles = find_dest_files(dest,fn_ptrns)
            numfound = log_foundfiles(fn_ptrn,xfiles)
            numfiles_zipped = compress(xfiles)
            if numfiles_zipped > 0:
                msg = dest + ': # of files written to zip file: ' + str(numfiles_zipped) + '.\n'
                log.write(msg)
                if validate(numfound,numfiles_zipped):
                    lp = dest_lps[dest]
                    load_file = os.path.join(xp,zipfilename)
                    move(load_file,lp)
                    log.write('Validation successful. Zip file: ' + zipfilename + ' moved to '+ lp + '.\n\n')
                    xfs += xfiles
        for xf in list(set(xfs)):
            os.remove(xf)
    except:
        log.write('Function ETL failure.\n')
        log.write(traceback.print_exc())
        

def log_foundfiles(fn_ptrn,xfiles):
    numfound = len(xfiles)
    log.write(str(numfound) + ' ' + fn_ptrn + ' files found in ' + xp + ':\n\n')
    for xf in xfiles:
        log.write(os.path.basename(xf) + '\n')
    log.write('\n')
    return numfound

def validate(numfound,numfiles_zipped):
    checkstatus = numfound == numfiles_zipped
    if not checkstatus:
        log.write('ERROR: ETL validation failure.')
        log.write('Files found: ' + str(numfound) + '.')
        log.write('Files zipped: ' + str(numfiles_zipped) + '.')
        log.write('File count discrepancy: ' + str(numfound - numfiles_zipped) + '.')
        osCommandString = 'notepad.exe ' + log.file
        os.system(osCommandString)
    return checkstatus

    
#xp,lps = set_test_paths(xp,lps)
log = start()
load_file = ETL()
log.write('job ended at ' + datetime.datetime.now().ctime() + '\n\n')