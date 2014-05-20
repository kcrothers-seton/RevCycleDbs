import os, glob, string, zipfile, datetime, time
from shutil import move
from shutil import copy
from file_hndlrs import File3,Txt3
import inspect,traceback


logpath = 'N:\\PFS-DSS\\RevCycleDBs\ssifiles\\logs\\835'

xp = '\\\\ausssi\\ssiapp\\renws\\billing\\uploadh\\835\\Today'
lps = {'*.835':'G:\\SHN-FTP-XFR\\IS-Apps\\RCPSI\\835',
       '*car*.835':'G:\\shn-ftp-xfr\\IS-Apps\\E-Scan'}

filetype = '835'


def start():
    global today
    today = datetime.datetime.today()
    log_fn = 'log_' + filetype + '_' + today.strftime('%Y-%m-%d_%H%M') + '.txt'
    global log
    log = Txt3(None,logpath,log_fn)
    log.write('Job started at ' + datetime.datetime.now().ctime() + '\n\n')
    return log

def ETL():
    try:
        xfs = []
        for fn_ptrn,lp in lps.items():
            num_zipped = 0
            xfiles = glob.glob(os.path.join(xp,fn_ptrn))
            numfound = log_foundfiles(fn_ptrn,xfiles)
            zipfilename = today.strftime('%y%m%d') + '_' + filetype + 's.zip'
            transform_file = os.path.join(xp,zipfilename)
            load_file = os.path.join(lp,zipfilename)
            for xf in xfiles:
                fn = os.path.basename(xf)
                f = File3(None,xp,fn)
                f.archive(xp,zipfilename,remove=False)
                num_zipped += 1
            if num_zipped > 0:
                log.write(str(num_zipped) + ' ' + fn_ptrn + ' files written to zip file: ' + zipfilename + '.\n')
                if validate(numfound,num_zipped):
                    move(transform_file,load_file)
                    log.write('Validation successful. Zip file: ' + zipfilename + ' moved to '+ lp + '.\n\n')
                    xfs += xfiles
        xfs = list(set(xfs))
        for xf in xfs:
            os.remove(xf)
    except:
        log.write('Function ETL failure.\n')
        log.write(traceback.print_exc())
    return load_file,xfs
        

def log_foundfiles(fn_ptrn,xfiles):
    numfound = len(xfiles)
    log.write(str(numfound) + ' ' + fn_ptrn + ' files found in ' + xp + ':\n\n')
    for xf in xfiles:
        log.write(os.path.basename(xf) + '\n')
    log.write('\n')
    return numfound

def validate(numfound,num_zipped):
    checkstatus = numfound == num_zipped
    if not checkstatus:
        log.write('ERROR: ETL validation failure.\n')
        log.write('Files found: ' + str(numfound) + '.\n')
        log.write('Files zipped: ' + str(num_zipped) + '.\n')
        log.write('File count discrepancy: ' + str(numfound - num_zipped) + '.\n')
        osCommandString = 'notepad.exe ' + log.file
        os.system(osCommandString)
    return checkstatus

def set_test_paths(xp,lps):
    xp = os.path.join(xp,'test')
    for k,lp in lps.items():
        lps[k] = os.path.join(lp,'test')
    return xp,lps

def reconcile(load_file,xfs):
    time.sleep(60)
    load_p,load_fn = os.path.split(load_file)
    arc_f = os.path.join(load_p,'Archive',load_fn)
    arc_z = zipfile.ZipFile(arc_f,'r')
    arc_zipped_files = file.namelist()
    rcpsi_zipped_files = [os.path.basename(x) for x in xfs]
    # num_check = len(rcpsi_zipped_files) == len(arc_zipped_files)
    names_check = rcpsi_zipped_files == arc_zipped_files
    if not names_check:
        reconcil = {'RCPSI':[rcpsi_zipped_files],
                   'IS':[arc_zipped_files]}           
        reconcil['RCPSI'].append([x for x in rcpsi_zipped_files if x not in arc_zipped_files])
        reconcil['IS'].append([x for x in arc_zipped_files if x not in rcpsi_zipped_files])
    if not names_check:
        log.write('ERROR: IS ftp reconciliation failure.')
        log.write('RCPSI loaded files found: ' + str(len(reconcil['RCPSI'][0])) + '.\n')
        log.write('IS archived files found: ' + str(len(reconcil['IS'][0])) + '.\n\n')
        if reconcil['RCPSI'][1]:
            log.write('RCPSI files not found in IS: ' + str(len(reconcil['RCPSI'][1])) + '.\n')
            for x in reconcil['RCPSI'][1]:
                log.write(x + '\n')
            log.write('\n')
        if reconcil['IS'][1]:
            log.write('IS files not found in RCPSI: ' + str(len(reconcil['IS'][1])) + '.\n')
            for x in reconcil['IS'][1]:
                log.write(x + '\n')
            log.write('\n')
        osCommandString = 'notepad.exe ' + log.file
        os.system(osCommandString)
    return reconciliation_status

#xp,lps = set_test_paths(xp,lps)
log = start()
load_file,xfs = ETL()
reconciliation_status = reconcile(load_file,xfs)
log.write('job ended at ' + datetime.datetime.now().ctime() + '\n\n')
